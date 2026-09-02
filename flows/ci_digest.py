"""ChannelHub — 竞品情报：把一段时间的提及交给 Claude 出一份自然语言简报。

输入 raw.ci_mention（reddit / youtube / mydealz / amazon / instagram / 媒体），
输出 mart.ci_digest 一行。周频，跑在 ci-media（周一 06:00 UTC）之后。

与其它 ci_* flow 的两处刻意差异：
  1) **dry-run 语义不同**。别的 flow 干跑是「照常抓取解析、不写库」，因为抓取
     不花钱。这里照常调用就是照常付费，所以 CI_DRY_RUN=true 时**不调 LLM**，
     只打印会送多少条、多少 token。验证取数口径不需要真的烧一次钱。
  2) **不截断输入**。提及量超过 CI_DIGEST_MAX_INPUT_TOKENS 时宁可失败告警，
     也不悄悄砍掉一部分 —— 「基于 60% 的数据写出的简报」和完整简报长得一模一样，
     没人能从结果里看出被砍过，这种错误必须在入口处显式暴露。
"""

from __future__ import annotations

from datetime import date

from ci_common import _env, _pg, is_dry_run, maybe_alert
from prefect import flow, get_run_logger, task

MODEL = "claude-opus-5"

SYSTEM = """你是一名竞品情报分析师，服务于一家在德国市场销售擦窗机器人的公司。

你会收到一批从公开渠道采集的「提及」（Reddit / YouTube 视频与评论 / mydealz 优惠帖 /
Amazon 评论 / Instagram 贴文 / 德语媒体评测）。每条标注了来源、日期、命中的产品、
互动数据。原文以德语为主，也有英语。

写一份中文简报，只讲数据里真实出现的内容。要求：

1. **总体声量**：这段时间讨论集中在哪些产品、哪些渠道，跟数据里能看出的前期相比如何。
2. **用户在意什么**：反复出现的具体话题（清洁效果、噪音、边角覆盖、安全绳、App、
   续航、价格），给出你判断的依据。
3. **正负面**：分别举出具体例子，注明来源与日期。
4. **价格与促销**：出现的价格点、折扣、渠道。
5. **竞品动向**：竞品被讨论的方式与自家有何不同。
6. **值得注意的新信息**：新品、新评测、异常投诉、突发声量。

硬性要求：
- 每条判断都要能追溯到具体提及，可直接引用原文短句（保留德语原文并附中文翻译）。
- 样本量小的时候要明说「仅 N 条，不足以下结论」，不要把 3 条评论写成趋势。
- 数据里没有的，就写「本期数据未覆盖」。不要补充你自己知道的行业背景。
- 标了 `[旧文补录]` 的是我们本期**新发现**、但很早就发表的内容。它仍然是新情报，
  但**不能当成本期的新动向**——写的时候要点明发表时间。
- 不要复述提及列表，要给出结论。"""


def _load_mentions(window_days: int) -> list[dict]:
    """窗口内的提及。按产品和时间排序，让同一款的讨论在提示里挨在一起。

    !! 按 ingested_at 过滤，不是 published_at。!!
    简报要回答的是「这周我们**新掌握**了什么」，不是「这周发表了什么」。媒体评测
    和 Reddit 老帖的 published_at 可以是一年前（实测库里最早 2025-02），但我们
    这周才发现它 —— 那对情报来说就是新信息。用 published_at 过滤会把这类整批漏掉，
    而且漏得毫无痕迹（简报照样生成，只是内容少一半）。
    published_at 仍然送进提示里，让模型自己判断「这是我们新发现的旧文章」。

    与 mart.v_ci_share_of_voice 的口径差异是**故意的**：那个视图量的是声量随时间
    的分布，按发布日分桶才对；这里是工作简报，按发现日才对。
    """
    sql = """
        SELECT m.source_code, p.display_name, p.is_own,
               m.published_at::date AS published_on,
               m.ingested_at::date  AS ingested_on,
               (m.published_at < now() - make_interval(days => %s)) AS is_backfill,
               m.title, m.body, m.engagement::text, m.url
        FROM raw.ci_mention m
        JOIN core.ci_product p ON p.product_id = m.product_id
        WHERE m.ingested_at > now() - make_interval(days => %s)
        ORDER BY p.display_name, m.published_at, m.source_code
    """
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (window_days, window_days))
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


def _render(rows: list[dict]) -> str:
    out = []
    for r in rows:
        own = "自家" if r["is_own"] else "竞品"
        # 旧文章明确标注，否则模型会把一篇 2025 年的评测当成本周新动向
        age = "[旧文补录]" if r["is_backfill"] else ""
        head = (f"[发表{r['published_on']}][采集{r['ingested_on']}]{age}"
                f"[{r['source_code']}][{own}:{r['display_name']}]")
        text = (r["title"] or "") + ("\n" + r["body"] if r["body"] else "")
        out.append(f"{head} 互动={r['engagement']} {r['url'] or ''}\n{text.strip()}")
    return "\n\n---\n\n".join(out)


@task(retries=1, retry_delay_seconds=120)
def build_digest(window_days: int) -> dict:
    logger = get_run_logger()
    rows = _load_mentions(window_days)
    stats = {"mentions": len(rows), "input_tokens": 0, "output_tokens": 0, "written": 0}

    if not rows:
        # 不是错误：这个品类本来就冷，一周零声量是可能的。不调 LLM、不写空记录 ——
        # 看板上的空档如实反映「那周确实没人讨论」。
        logger.info("窗口内 0 条提及，跳过（不生成空简报）")
        return stats

    body = _render(rows)
    sources = sorted({r["source_code"] for r in rows})
    user_msg = (f"以下是最近 {window_days} 天**采集到**的 {len(rows)} 条提及"
                f"（按发表时间排序；发表时间可能远早于采集时间）：\n\n{body}")

    import anthropic
    client = anthropic.Anthropic()          # 凭证从 ANTHROPIC_API_KEY 解析

    # 入口处校验规模。超限显式失败，不截断（见模块 docstring 第 2 点）。
    ceiling = int(_env("CI_DIGEST_MAX_INPUT_TOKENS") or "400000")
    counted = client.messages.count_tokens(
        model=MODEL, system=SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    ).input_tokens
    stats["input_tokens"] = counted
    logger.info("本期 %d 条提及 / %d input tokens / 源: %s", len(rows), counted, sources)
    if counted > ceiling:
        raise RuntimeError(
            f"输入 {counted} tokens 超过上限 {ceiling}。宁可失败也不截断 —— "
            f"请调大 CI_DIGEST_MAX_INPUT_TOKENS，或缩短 CI_DIGEST_WINDOW_DAYS。"
        )

    if is_dry_run():
        logger.warning("CI_DRY_RUN=true —— 不调用 LLM（调用即计费）。"
                       "本应送 %d 条 / %d tokens 给 %s", len(rows), counted, MODEL)
        return stats

    resp = client.beta.messages.create(
        model=MODEL,
        max_tokens=16000,
        system=SYSTEM,
        thinking={"type": "adaptive"},
        # 拒答兜底：分类器误判时服务端换模型重跑，不至于让一次定时任务空手而归。
        # 本用例（德语家电讨论）几乎不可能触发，留着是保险，去掉也不影响功能。
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        messages=[{"role": "user", "content": user_msg}],
    )

    # 读 content 前先看 stop_reason：整条链都拒答时 content 里没有可用正文。
    if resp.stop_reason == "refusal":
        detail = getattr(resp.stop_details, "explanation", "") or ""
        raise RuntimeError(f"LLM 拒答（含兜底模型）: {detail}")

    summary = "\n".join(b.text for b in resp.content if b.type == "text").strip()
    if not summary:
        raise RuntimeError(f"LLM 未返回正文，stop_reason={resp.stop_reason}")

    stats["input_tokens"] = resp.usage.input_tokens
    stats["output_tokens"] = resp.usage.output_tokens

    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO mart.ci_digest "
                "(digest_on, window_days, scope, mention_cnt, source_codes, summary, "
                " model, input_tokens, output_tokens) "
                "VALUES (%s,%s,'all',%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (digest_on, window_days, scope) DO UPDATE SET "
                "  mention_cnt = EXCLUDED.mention_cnt, "
                "  source_codes = EXCLUDED.source_codes, "
                "  summary = EXCLUDED.summary, "
                "  model = EXCLUDED.model, "
                "  input_tokens = EXCLUDED.input_tokens, "
                "  output_tokens = EXCLUDED.output_tokens, "
                "  generated_at = now()",
                (date.today(), window_days, len(rows), sources, summary,
                 resp.model, resp.usage.input_tokens, resp.usage.output_tokens),
            )
        conn.commit()
    stats["written"] = 1
    return stats


@flow(name="ci-digest")
def ci_digest() -> dict:
    logger = get_run_logger()
    window_days = int(_env("CI_DIGEST_WINDOW_DAYS") or "7")
    try:
        stats = build_digest(window_days)
    except Exception as e:
        logger.warning("简报生成失败: %s", e, exc_info=True)
        try:
            maybe_alert("digest", "digest_failed", f"{type(e).__name__}: {e}", logger)
        except Exception:
            logger.warning("告警本身也失败了")
        raise
    logger.info("ci-digest 汇总: %s", stats)
    return stats


if __name__ == "__main__":
    ci_digest()

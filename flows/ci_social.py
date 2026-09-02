"""ChannelHub — 竞品情报：用户讨论层日频采集。

五个源，全部写 raw.ci_mention：
  · Reddit    OAuth application-only + REST(不引 praw:一个 token + 一个 GET 而已)
  · YouTube   Data API v3 REST(不引 google-api-python-client，同上)
  · mydealz   Pepper 官方 REST /thread/search + /thread/{id}/comments
  · Amazon    /product-reviews/{ASIN} 评论正文(路径在 robots 白名单内)
  · Instagram Graph Hashtag Search(观察清单在 core.ci_hashtag;配额与无作者的
              约束见下方该节文件头，它跟其它四个源有结构性差异)

文档语境用 match_products_all()：一篇对比讨论同时挂到多款上，这正是
raw.ci_mention 唯一键含 product_id 的原因。单品语境(商品页)另走 match_product()。

GDPR：作者一律只存 author_hash，绝不落库显示名。
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

from ci_common import (
    FetchBlocked,
    _env,
    _flag,
    _pg,
    author_hash,
    fetch,
    is_dry_run,
    load_products,
    looks_like_bot_wall,
    match_products_all,
    maybe_alert,
    snapshot,
)
from ci_price import _active_sources  # 单一实现，避免两份 active 判定
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

SUBREDDITS = ("staubsaugerroboter", "de", "Haushalt", "smarthome", "wohnen")


def _write_mentions(rows) -> int:
    """rows: (product_id, source_code, external_id, url, title, body, lang,
              author_hash, published_at, engagement_json, snapshot_id, run_id)

    正文不可变;只有 engagement(播放量/点赞会长)允许刷新。
    """
    if not rows:
        return 0
    if is_dry_run():
        return len(rows)          # 干跑报「本该写入多少条」，否则这轮验证等于没验
    sql = ("INSERT INTO raw.ci_mention (product_id, source_code, external_id, url, title, body, "
           " lang, author_hash, published_at, engagement, snapshot_id, ingestion_run_id) "
           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           "ON CONFLICT (source_code, external_id, product_id) DO UPDATE SET "
           "  engagement = EXCLUDED.engagement, engagement_updated_at = now()")
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    return len(rows)


def _fan_out(products, text, make_row) -> list:
    """一篇文档 → 每个命中产品一行。没命中则不入库(噪音不进事实表)。"""
    return [make_row(pid) for pid in match_products_all(text, products)]


def _ts(epoch_or_iso) -> datetime | None:
    if epoch_or_iso is None:
        return None
    try:
        if isinstance(epoch_or_iso, (int, float)):
            return datetime.fromtimestamp(epoch_or_iso, tz=timezone.utc)
        return datetime.fromisoformat(str(epoch_or_iso).replace("Z", "+00:00"))
    except (ValueError, OSError):
        return None


# ===========================================================================
# Reddit
# ===========================================================================
@task(retries=2, retry_delay_seconds=30)
def collect_reddit(run_id: str) -> dict:
    logger = get_run_logger()
    stats = {"queries": 0, "mentions": 0}
    cid, csec = _env("REDDIT_CLIENT_ID"), _env("REDDIT_CLIENT_SECRET")
    if not (cid and csec):
        logger.info("Reddit: 未配置 REDDIT_CLIENT_ID/SECRET，跳过")
        return stats

    import base64

    import httpx
    ua = f"ChannelHub-CI/1.0 (contact {_env('CI_CONTACT_EMAIL') or _env('ALERT_EMAIL_TO')})"
    tok = httpx.post("https://www.reddit.com/api/v1/access_token",
                     headers={"Authorization": "Basic " + base64.b64encode(f"{cid}:{csec}".encode()).decode(),
                              "User-Agent": ua},
                     data={"grant_type": "client_credentials"}, timeout=30)
    tok.raise_for_status()
    access = tok.json()["access_token"]

    products = load_products()
    rows = []
    for sub in SUBREDDITS:
        for p in products:
            stats["queries"] += 1
            r = httpx.get(f"https://oauth.reddit.com/r/{sub}/search",
                          headers={"Authorization": f"Bearer {access}", "User-Agent": ua},
                          params={"q": p.display_name, "restrict_sr": 1,
                                  "sort": "new", "limit": 50, "t": "year"}, timeout=30)
            if r.status_code != 200:
                logger.warning("Reddit r/%s HTTP %s", sub, r.status_code)
                continue
            for child in r.json().get("data", {}).get("children", []):
                d = child.get("data", {})
                text = f"{d.get('title','')}\n{d.get('selftext','')}"
                eng = json.dumps({"score": d.get("score"), "comments": d.get("num_comments"),
                                  "subreddit": sub})
                rows += _fan_out(products, text, lambda pid, d=d, text=text, eng=eng: (
                    pid, "reddit", d.get("name") or d.get("id"),
                    "https://www.reddit.com" + (d.get("permalink") or ""),
                    (d.get("title") or "")[:1000], d.get("selftext"), "de",
                    author_hash(d.get("author")), _ts(d.get("created_utc")),
                    eng, None, run_id))
    stats["mentions"] = _write_mentions(rows)
    return stats


# ===========================================================================
# YouTube
# ===========================================================================
@task(retries=2, retry_delay_seconds=30)
def collect_youtube(run_id: str) -> dict:
    logger = get_run_logger()
    stats = {"queries": 0, "mentions": 0, "comments": 0}
    key = _env("YOUTUBE_API_KEY")
    if not key:
        logger.info("YouTube: 未配置 YOUTUBE_API_KEY，跳过")
        return stats

    import httpx
    base = "https://www.googleapis.com/youtube/v3"
    products = load_products()
    rows = []
    for p in products:
        stats["queries"] += 1
        r = httpx.get(f"{base}/search", params={
            "part": "snippet", "q": p.display_name, "type": "video",
            "regionCode": "DE", "relevanceLanguage": "de", "maxResults": 25,
            "order": "date", "key": key}, timeout=30)
        if r.status_code != 200:
            logger.warning("YouTube search HTTP %s: %s", r.status_code, r.text[:200])
            continue
        vids = [it["id"]["videoId"] for it in r.json().get("items", [])
                if it.get("id", {}).get("videoId")]
        if not vids:
            continue

        rs = httpx.get(f"{base}/videos", params={
            "part": "snippet,statistics", "id": ",".join(vids), "key": key}, timeout=30)
        if rs.status_code != 200:
            continue
        for v in rs.json().get("items", []):
            sn, st = v.get("snippet", {}), v.get("statistics", {})
            text = f"{sn.get('title','')}\n{sn.get('description','')}"
            eng = json.dumps({"views": st.get("viewCount"), "likes": st.get("likeCount"),
                              "comments": st.get("commentCount"),
                              "channel": sn.get("channelTitle")})
            rows += _fan_out(products, text, lambda pid, v=v, sn=sn, text=text, eng=eng: (
                pid, "youtube", v["id"], f"https://www.youtube.com/watch?v={v['id']}",
                (sn.get("title") or "")[:1000], sn.get("description"),
                sn.get("defaultAudioLanguage") or "de",
                author_hash(sn.get("channelTitle")), _ts(sn.get("publishedAt")),
                eng, None, run_id))

            # 视频评论:讨论层的真正内容所在
            rc = httpx.get(f"{base}/commentThreads", params={
                "part": "snippet", "videoId": v["id"], "maxResults": 50,
                "order": "relevance", "textFormat": "plainText", "key": key}, timeout=30)
            if rc.status_code != 200:
                continue
            for c in rc.json().get("items", []):
                top = c.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
                body = top.get("textDisplay") or ""
                # 评论继承所属视频的产品归属：评论本身常只说「它」
                rows += _fan_out(products, text + "\n" + body,
                                 lambda pid, c=c, top=top, body=body, v=v: (
                    pid, "youtube", "c:" + c["id"],
                    f"https://www.youtube.com/watch?v={v['id']}&lc={c['id']}",
                    None, body, "de", author_hash(top.get("authorDisplayName")),
                    _ts(top.get("publishedAt")),
                    json.dumps({"likes": top.get("likeCount")}), None, run_id))
                stats["comments"] += 1
    stats["mentions"] = _write_mentions(rows)
    return stats


# ===========================================================================
# mydealz（官方 REST；讨论区是德国消费者主场）
# ===========================================================================
# ===========================================================================
# mydealz：公开 RSS 分组 feed（不走需签名的 REST，理由见 ci_price）
# ===========================================================================
@task(retries=2, retry_delay_seconds=30)
def collect_mydealz_threads(run_id: str) -> dict:
    """把 deal 帖本身作为一条提及入库（标题 + 摘要 + 热度）。

    评论正文需要另抓 /deals/<slug> 页面解析（robots 允许），本轮未做 ——
    帖子层面的声量与促销事件已足够支撑看板，评论留到 LLM 富化那一期一起做。
    """
    from ci_price import (  # 同业务线内复用，避免两份实现
        mydealz_groups,
        parse_mydealz_rss,
    )

    logger = get_run_logger()
    stats = {"pages": 0, "items": 0, "mentions": 0}
    products = load_products()
    rows = []
    for group in mydealz_groups():
        url = f"https://www.mydealz.de/rss/gruppe/{group}"
        try:
            status, body, ctype = fetch(url, mode="http")
        except FetchBlocked as e:
            logger.warning("mydealz %s 被拦: %s", group, e)
            continue
        if status != 200:
            logger.warning("mydealz %s HTTP %s", group, status)
            continue
        stats["pages"] += 1
        snap = snapshot("mydealz", url, status, body, ctype, run_id=run_id)
        items = parse_mydealz_rss(body)
        stats["items"] += len(items)
        for it in items:
            eng = json.dumps({"merchant": it.get("merchant"),
                              "price_cents": it.get("price_cents"),
                              "group": group})
            rows += _fan_out(products, it["title"],
                             lambda pid, it=it, eng=eng, snap=snap: (
                pid, "mydealz", it["guid"] or it["link"], it["link"],
                it["title"][:1000], None, "de", None,
                _ts(_rfc822(it.get("published"))), eng, snap, run_id))
    stats["mentions"] = _write_mentions(rows)
    return stats


def _rfc822(text):
    if not text:
        return None
    from email.utils import parsedate_to_datetime
    try:
        return parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None


# ===========================================================================
# Amazon 评论正文（/product-reviews/{ASIN}，在 robots 白名单内）
# ===========================================================================
_REV_BLOCK_RE = re.compile(r'data-hook="review"[^>]*id="([^"]+)"(.*?)(?=data-hook="review"|</div>\s*</div>\s*</div>)', re.DOTALL)
_REV_BODY_RE = re.compile(r'data-hook="review-body"[^>]*>(.*?)</span>', re.DOTALL)
_REV_STAR_RE = re.compile(r'data-hook="review-star-rating"[^>]*>.*?([0-9],[0-9])\s*von\s*5', re.DOTALL)
_REV_DATE_RE = re.compile(r'data-hook="review-date"[^>]*>([^<]+)</span>', re.DOTALL)


def parse_amazon_reviews(html: str) -> list[dict]:
    out = []
    for rid, block in _REV_BLOCK_RE.findall(html):
        mb = _REV_BODY_RE.search(block)
        if not mb:
            continue
        text = re.sub(r"<[^>]+>", " ", mb.group(1))
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            continue
        ms = _REV_STAR_RE.search(block)
        md = _REV_DATE_RE.search(block)
        out.append({"id": rid, "body": text,
                    "stars": float(ms.group(1).replace(",", ".")) if ms else None,
                    "date_text": md.group(1).strip() if md else None})
    return out


@task(retries=1, retry_delay_seconds=30)
def collect_amazon_reviews(run_id: str) -> dict:
    logger = get_run_logger()
    stats = {"pages": 0, "mentions": 0, "blocked": 0}
    if not _flag("CI_AMAZON_ENABLED", "true"):
        logger.info("CI_AMAZON_ENABLED=false —— 跳过 Amazon 评论")
        return stats
    rows = []
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT product_id, external_id FROM core.ci_product_alias "
                        "WHERE source_code='amazon_de'")
            pairs = cur.fetchall()
    for product_id, asin in pairs:
        url = f"https://www.amazon.de/product-reviews/{asin}"
        try:
            status, body, ctype = fetch(url, mode="impersonate")
        except FetchBlocked as e:
            stats["blocked"] += 1
            logger.warning("Amazon 评论被拦: %s", e)
            continue
        html = body.decode("utf-8", "replace")
        if looks_like_bot_wall(html, status):
            stats["blocked"] += 1
            logger.warning("Amazon 评论页命中反爬墙 (status=%s)", status)
            continue
        stats["pages"] += 1
        snap = snapshot("amazon_de", url, status, body, ctype,
                        product_id=product_id, run_id=run_id)
        # 商品页评论的产品归属由 ASIN 决定，无需再从正文猜
        for rev in parse_amazon_reviews(html):
            rows.append((product_id, "amazon_de", f"r:{rev['id']}", url, None, rev["body"],
                         "de", None, None,
                         json.dumps({"stars": rev["stars"], "date_text": rev["date_text"]}),
                         snap, run_id))
    stats["mentions"] = _write_mentions(rows)
    return stats


# ===========================================================================
# Instagram（Hashtag Search API —— 官方唯一能按词检索公开内容的入口）
# ===========================================================================
# 这条线跟其它源有三处**结构性**不同，读代码前先知道，否则会以为是 bug：
#   1) 拿不到作者。文档明写 username 字段不可请求，故 author_hash 恒为 NULL。
#      副作用：mart.v_ci_share_of_voice 的 author_cnt 对本源恒为 0，
#      看板上要按 source_code 排除，别当成「没人讨论」。
#   2) recent_media 只回**查询时刻前 24 小时**内发布的贴文。补不了历史，
#      漏一天就是真的少一天 —— 所以本源的失败必须告警，不能静默跳过。
#   3) 配额是稀缺资源：每账号 7 天滚动窗口内最多 30 个不同 hashtag。
#      观察清单在 core.ci_hashtag，配额记账见 _ig_pick_hashtags()。
IG_MEDIA_FIELDS = "id,media_type,caption,comments_count,like_count,media_url,permalink,timestamp"


def _ig_get(path: str, params: dict, token: str, version: str) -> tuple[int, dict]:
    """Graph API GET → (http_status, json)。

    刻意不 raise_for_status：Graph 的错误信息全在 JSON body 的 error.code 里
    （190=令牌失效、4/613=撞限流、10/200=权限没批下来），这三类的处置完全不同，
    必须拿到 body 才能分诊。只看 HTTP 码会把它们混成一个「HTTP 400」。
    """
    import httpx
    url = f"https://graph.facebook.com/{version}/{path}"
    r = httpx.get(url, params={**params, "access_token": token}, timeout=30)
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, {"error": {"message": f"非 JSON 响应 (HTTP {r.status_code})",
                                         "code": -1}}


def _ig_safe_url(path: str, params: dict, version: str) -> str:
    """存档/留痕用的 URL —— **绝不能带 access_token**。

    snapshot() 会把 URL 原样写进 raw.ci_snapshot 并长期保留，令牌落库等于泄漏。
    """
    from urllib.parse import urlencode
    return f"https://graph.facebook.com/{version}/{path}?" + urlencode(params)


def _ig_pick_hashtags(budget: int) -> tuple[list[tuple[str, str | None]], list[str]]:
    """按配额挑出本轮要查的词。返回 (要查的, 因配额被推迟的)。

    Meta 限「7 天滚动窗口内 30 个不同 hashtag」。**窗口内已经查过的词再查不额外
    占配额**，所以顺序必须是：先跑窗口内的老词（免费），剩余预算才分给新词。
    反过来先跑新词，会把预算浪费在还没验证过产出的词上，还可能把正在跑的老词挤掉，
    在时间序列上造成断点 —— 声量曲线断一天比少一个新词严重得多。
    """
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hashtag, ig_hashtag_id, "
                "       (last_queried_at > now() - interval '7 days') AS in_window "
                "FROM core.ci_hashtag WHERE active ORDER BY hashtag")
            rows = cur.fetchall()
    in_window = [(h, i) for h, i, w in rows if w]
    fresh = [(h, i) for h, i, w in rows if not w]
    room = max(0, budget - len(in_window))
    return in_window + fresh[:room], [h for h, _ in fresh[room:]]


def _ig_remember(hashtag: str, ig_id: str | None, media_count: int) -> None:
    """记账：缓存 id + 推进配额窗口。dry-run 下不写，配额账本也就不动。"""
    if is_dry_run():
        return
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE core.ci_hashtag SET "
                "  ig_hashtag_id = coalesce(%s, ig_hashtag_id), "
                "  first_queried_at = coalesce(first_queried_at, now()), "
                "  last_queried_at = now(), "
                "  last_media_count = %s "
                "WHERE hashtag = %s",
                (ig_id, media_count, hashtag))
        conn.commit()


@task(retries=1, retry_delay_seconds=60)
def collect_instagram_hashtags(run_id: str) -> dict:
    """core.ci_hashtag 观察清单 → recent_media → raw.ci_mention。

    retries 只给 1 次（其它源是 2）：每次重试都可能再动一次配额窗口，
    而本源的失败模式多半是令牌/权限问题，重试救不回来。
    """
    logger = get_run_logger()
    stats = {"hashtags": 0, "deferred": 0, "media": 0, "mentions": 0, "auth_error": 0}

    token = _env("INSTAGRAM_ACCESS_TOKEN")
    ig_user_id = _env("INSTAGRAM_IG_USER_ID")
    if not (token and ig_user_id):
        logger.info("Instagram: 未配置 INSTAGRAM_ACCESS_TOKEN/INSTAGRAM_IG_USER_ID，跳过")
        return stats
    version = _env("INSTAGRAM_GRAPH_VERSION") or "v26.0"
    budget = int(_env("CI_INSTAGRAM_HASHTAG_BUDGET") or "26")

    todo, deferred = _ig_pick_hashtags(budget)
    stats["deferred"] = len(deferred)
    if deferred:
        # 不是错误，但必须看得见：说明清单长过配额，有词长期取不到数。
        logger.warning("Instagram 配额不足，本轮推迟 %d 个词: %s", len(deferred), ", ".join(deferred))

    products = load_products()
    rows = []
    for hashtag, cached_id in todo:
        # 1) hashtag 名 → 永久 id（缓存命中就省这一次调用）
        ig_id = cached_id
        if not ig_id:
            status, js = _ig_get("ig_hashtag_search",
                                 {"user_id": ig_user_id, "q": hashtag}, token, version)
            err = js.get("error")
            if err:
                code = err.get("code")
                if code in (190, 102, 10, 200, 803):
                    # 令牌失效或权限没批 —— 整条线都别再试了，继续跑只会把配额和日志刷满
                    stats["auth_error"] += 1
                    logger.error("Instagram 鉴权/权限失败 (code=%s): %s", code, err.get("message"))
                    break
                logger.warning("Instagram hashtag_search '%s' 失败 (code=%s): %s",
                               hashtag, code, err.get("message"))
                continue
            data = js.get("data") or []
            if not data:
                logger.warning("Instagram hashtag '%s' 查无此标签", hashtag)
                _ig_remember(hashtag, None, 0)
                continue
            ig_id = data[0]["id"]

        # 2) recent_media（只有 24h 内的贴文；分页上限 50/页）
        params = {"user_id": ig_user_id, "fields": IG_MEDIA_FIELDS, "limit": 50}
        status, js = _ig_get(f"{ig_id}/recent_media", params, token, version)
        err = js.get("error")
        if err:
            code = err.get("code")
            if code in (190, 102, 10, 200, 803):
                stats["auth_error"] += 1
                logger.error("Instagram 鉴权/权限失败 (code=%s): %s", code, err.get("message"))
                break
            logger.warning("Instagram recent_media '%s' 失败 (code=%s): %s",
                           hashtag, code, err.get("message"))
            continue

        stats["hashtags"] += 1
        media = js.get("data") or []
        stats["media"] += len(media)
        # 存档用不带令牌的 URL（见 _ig_safe_url）
        snap = snapshot("instagram",
                        _ig_safe_url(f"{ig_id}/recent_media", params, version),
                        status, json.dumps(js, ensure_ascii=False).encode(),
                        "application/json", run_id=run_id)

        for m in media:
            caption = m.get("caption") or ""
            if not caption:
                continue          # 纯图无文案：型号消歧无从下手，不入库（噪音不进事实表）
            eng = json.dumps({"likes": m.get("like_count"),
                              "comments": m.get("comments_count"),
                              "media_type": m.get("media_type"),
                              "hashtag": hashtag})
            rows += _fan_out(products, caption,
                             lambda pid, m=m, caption=caption, eng=eng, snap=snap: (
                pid, "instagram", m["id"], m.get("permalink"),
                caption[:1000],           # 标题位放文案首段，明细表里好扫
                caption, "de",
                None,                     # 作者：API 不给 username，见文件头第 1 点
                _ts(m.get("timestamp")), eng, snap, run_id))

        _ig_remember(hashtag, ig_id, len(media))

    stats["mentions"] = _write_mentions(rows)
    return stats


# ===========================================================================
# 编排
# ===========================================================================
@flow(name="ci-social")
def ci_social() -> dict:
    logger = get_run_logger()
    run_id = str(getattr(flow_run, "id", "") or "")
    total = {"sources": 0, "failed": 0, "mentions": 0, "blocked": 0, "auth_error": 0,
             "deferred": 0}
    if is_dry_run():
        logger.warning("CI_DRY_RUN=true —— 照常抓取解析但不写库、不存 MinIO")

    active = _active_sources()
    for name, job in (("reddit", lambda: collect_reddit(run_id)),
                      ("youtube", lambda: collect_youtube(run_id)),
                      ("mydealz", lambda: collect_mydealz_threads(run_id)),
                      ("amazon_de", lambda: collect_amazon_reviews(run_id)),
                      ("instagram", lambda: collect_instagram_hashtags(run_id))):
        if name not in active:
            logger.info("%s 在 core.ci_source 中已停用，跳过", name)
            continue
        total["sources"] += 1
        try:
            st = job()
            total["mentions"] += st.get("mentions", 0)
            total["blocked"] += st.get("blocked", 0)
            total["auth_error"] += st.get("auth_error", 0)
            total["deferred"] += st.get("deferred", 0)
            logger.info("%s 完成: %s", name, st)
            if st.get("blocked"):
                maybe_alert(name, "bot_wall", f"{st['blocked']} 个页面被拦。", logger)
            if st.get("auth_error"):
                maybe_alert(name, "api_auth_required",
                            "接口要求鉴权/签名，该源当前不可用；0 条结果不代表没人讨论。",
                            logger)
        except Exception as e:
            total["failed"] += 1
            logger.warning("%s 失败: %s", name, e, exc_info=True)
            try:
                maybe_alert(name, "collect_failed", f"{type(e).__name__}: {e}", logger)
            except Exception:
                logger.warning("%s 告警本身也失败了", name)

    logger.info("ci-social 汇总: %s", total)
    if total["failed"]:
        raise RuntimeError(f"{total['failed']} 个源采集失败: {total}")
    return total


if __name__ == "__main__":
    ci_social()

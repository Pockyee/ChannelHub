"""ChannelHub — 竞品情报：媒体评测层采集（周频 + 一次性历史回填）。

发现入口刻意**不逐家猜 RSS 地址**：Chip/Heise/Computerbild/connect/imtest/
FAZ Kaufkompass/Stiftung Warentest 各自的 feed 路径会改，猜错的后果是静默漏采
(没有报错、只是永远没有数据)。改用 Google News RSS 做统一发现：
  https://news.google.com/rss/search?q=...&hl=de&gl=DE&ceid=DE:de
再按 <source url> 的域名归属到 core.ci_source 里登记的媒体源;未登记的落 'other'
但仍入库 —— 漏掉一家没登记的媒体比错归类更可惜。
需要直连某家 feed 时用 CI_MEDIA_EXTRA_FEEDS(逗号分隔)补充，无需改代码。

正文抽取是**尽力而为**，覆盖记录不依赖它：
  · Google News 的 <link> 是 news.google.com/rss/articles/… 跳转地址，而该路径
    被 news.google.com 的 robots.txt 禁止 —— 所以不去解析它。RSS 条目自带的
    标题 + <source url> 出版方 + 发布时间，已足够支撑「哪家媒体何时评了哪款」，
    也就是 mart.v_ci_media_coverage 要的东西，摘要作为正文入库。
  · 只有链接指向出版方真实域名时(即 CI_MEDIA_EXTRA_FEEDS 直连的 feed)才去取
    全文，用 trafilatura 去模板抽正文(德语表现好)。
这样媒体覆盖当天就有数据，全文是锦上添花而非前置条件。
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

from ci_common import (
    FetchBlocked,
    _env,
    _pg,
    fetch,
    is_dry_run,
    load_products,
    looks_like_bot_wall,
    match_products_all,
    maybe_alert,
    snapshot,
)
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"


def _media_domains() -> dict[str, str]:
    """core.ci_source 里登记的媒体源 → {域名: source_code}。"""
    out = {}
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_code, base_url FROM core.ci_source "
                        "WHERE layer='media' AND active AND base_url IS NOT NULL")
            for code, base in cur.fetchall():
                host = urlparse(base).netloc.lower().removeprefix("www.")
                if host:
                    out[host] = code
    return out


def _outlet_for(url: str, source_url: str, domains: dict[str, str]) -> str:
    for cand in (source_url, url):
        host = urlparse(cand or "").netloc.lower().removeprefix("www.")
        for dom, code in domains.items():
            if host == dom or host.endswith("." + dom):
                return code
    return "other_media"


def _pub(text: str | None) -> datetime | None:
    if not text:
        return None
    try:
        return parsedate_to_datetime(text)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None


def parse_rss(xml_bytes: bytes) -> list[dict]:
    """RSS/Atom → [{title, link, summary, published, source_url}]（只用标准库）。"""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    items = []
    for it in root.iter():
        tag = it.tag.split("}")[-1]
        if tag not in ("item", "entry"):
            continue
        d = {"title": None, "link": None, "summary": None, "published": None, "source_url": ""}
        for ch in it:
            ctag = ch.tag.split("}")[-1]
            if ctag == "title":
                d["title"] = (ch.text or "").strip()
            elif ctag == "link":
                d["link"] = (ch.text or ch.attrib.get("href") or "").strip()
            elif ctag in ("description", "summary", "content"):
                d["summary"] = re.sub(r"<[^>]+>", " ", ch.text or "").strip()
            elif ctag in ("pubDate", "published", "updated"):
                d["published"] = (ch.text or "").strip()
            elif ctag == "source":
                d["source_url"] = ch.attrib.get("url", "") or (ch.text or "")
        if d["title"] and d["link"]:
            items.append(d)
    return items


def extract_article(html: str) -> str | None:
    """trafilatura 去模板抽正文;失败返回 None 由调用方退回 RSS 摘要。"""
    try:
        import trafilatura
    except ImportError:
        return None
    try:
        return trafilatura.extract(html, include_comments=False, favor_precision=True)
    except Exception:
        return None


def _write_mentions(rows) -> int:
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


@task(retries=2, retry_delay_seconds=30)
def discover(run_id: str) -> list[dict]:
    """Google News RSS + 可选直连 feed → 候选文章清单(已按标题粗筛)。"""
    logger = get_run_logger()
    products = load_products()
    seen, out = set(), []

    feeds = [(GOOGLE_NEWS_RSS,
              {"q": p.display_name, "hl": "de", "gl": "DE", "ceid": "DE:de"})
             for p in products]
    for extra in filter(None, (_env("CI_MEDIA_EXTRA_FEEDS") or "").split(",")):
        feeds.append((extra.strip(), None))

    for url, params in feeds:
        try:
            status, body, _ctype = fetch(url, mode="api", params=params)
        except FetchBlocked as e:
            logger.warning("feed 被拦 %s: %s", url, e)
            continue
        if status != 200:
            logger.warning("feed HTTP %s: %s", status, url)
            continue
        items = parse_rss(body)
        logger.info("%s → %d 条", urlparse(url).netloc, len(items))
        for it in items:
            if it["link"] in seen:
                continue
            seen.add(it["link"])
            out.append(it)
    return out


@task(retries=1, retry_delay_seconds=20)
def fetch_articles(candidates: list[dict], run_id: str) -> dict:
    logger = get_run_logger()
    stats = {"candidates": len(candidates), "matched": 0, "fulltext": 0,
             "summary_only": 0, "mentions": 0, "blocked": 0, "policy_skip": 0}
    products = load_products()
    domains = _media_domains()
    rows = []

    for it in candidates:
        # 标题+摘要即可判定归属，不必为此先抓一次全文
        pre = f"{it['title']}\n{it.get('summary') or ''}"
        pids = match_products_all(pre, products)
        if not pids:
            continue
        stats["matched"] += 1

        outlet = _outlet_for(it["link"], it.get("source_url", ""), domains)
        text, snap = it.get("summary"), None
        host = urlparse(it["link"]).netloc.lower()

        if host.endswith("news.google.com"):
            # 跳转地址被其 robots 禁止 —— 不是失败，是设计上就不取全文
            stats["summary_only"] += 1
        else:
            try:
                status, body, ctype = fetch(it["link"], mode="http")
                html = body.decode("utf-8", "replace")
                if looks_like_bot_wall(html, status):
                    stats["blocked"] += 1
                else:
                    snap = snapshot(outlet, it["link"], status, body, ctype, run_id=run_id)
                    full = extract_article(html)
                    if full:
                        text = full
                        stats["fulltext"] += 1
                    else:
                        stats["summary_only"] += 1
            except FetchBlocked as e:
                # robots/白名单拒绝是策略跳过，与被反爬墙拦下要分开计
                stats["policy_skip"] += 1
                logger.info("按策略跳过全文 %s: %s", it["link"], e)

        # 覆盖记录不依赖全文是否取到
        for pid in pids:
            rows.append((pid, outlet, it["link"], it["link"], it["title"][:1000],
                         text, "de", None, _pub(it.get("published")),
                         json.dumps({"discovered_via": "rss",
                                     "has_fulltext": bool(text and text != it.get("summary"))}),
                         snap, run_id))
    stats["mentions"] = _write_mentions(rows)
    return stats


@flow(name="ci-media")
def ci_media() -> dict:
    logger = get_run_logger()
    run_id = str(getattr(flow_run, "id", "") or "")
    if is_dry_run():
        logger.warning("CI_DRY_RUN=true —— 照常抓取解析但不写库、不存 MinIO")
    total = {"failed": 0}
    try:
        cands = discover(run_id)
        total.update(fetch_articles(cands, run_id))
        logger.info("ci-media 汇总: %s", total)
        if total.get("blocked"):
            maybe_alert("media", "bot_wall", f"{total['blocked']} 篇文章命中反爬墙。", logger)
        if total.get("matched") and not total.get("mentions"):
            # 匹配上了却一条都没入库 —— 写库路径坏了，不该静默
            maybe_alert("media", "zero_mentions",
                        f"匹配到 {total['matched']} 篇但入库 0 条。", logger)
        if total.get("candidates") and not total.get("matched"):
            # 有候选但一篇都匹配不上 —— 多半是消歧正则或产品主数据出了问题
            maybe_alert("media", "zero_matched",
                        f"发现 {total['candidates']} 条候选但一条都没匹配上。", logger)
    except Exception as e:
        total["failed"] = 1
        logger.warning("ci-media 失败: %s", e, exc_info=True)
        try:
            maybe_alert("media", "collect_failed", f"{type(e).__name__}: {e}", logger)
        except Exception:
            logger.warning("告警本身也失败了")
        raise RuntimeError(f"ci-media 失败: {e}") from e
    return total


if __name__ == "__main__":
    ci_media()

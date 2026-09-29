"""ChannelHub — 竞品情报：需求层采集（日频 flow，Trends 部分自带周频节流）。

价格 / 提及 / 广告都是「可见度」；这一层补**需求与存量**的代理，口径见
db/migrations/022_ci_demand.sql 文件头。三个源：

  · google_trends  品牌词在德国的 Google 搜索热度，5 年周序列。走网页背后的非官方接口
                   /trends/api/explore → /trends/api/widgetdata/multiline（robots.txt 只禁
                   /explore? 与 /trends/explore? 两个页面，接口路径不在禁止之列）。
                   一条情报线的所有词放**同一次请求**（≤ 5 个），值才可比。
                   每次都整段重取 5 年并按 fetched_on 留快照；距上次抓取不足
                   CI_TRENDS_MIN_DAYS（默认 6）天就跳过 —— 周序列没必要天天取，也少挨 429。
  · google_play    商店详情页 /store/apps/details 内嵌的 JSON（AF_initDataCallback ds:5）：
                   安装量档位 + 精确安装数 + 评分 + 评分数（全球口径）。
  · app_store      官方 iTunes lookup 接口，country=de：德国区评分 + 评分数。

刻意不采 App 评论：Play 的评论接口与 Apple 的评论 RSS 都在 robots.txt 的 Disallow 里（见 022）。

配置：core.ci_trend_term（搜索词）与 core.ci_app（App），CSV 即权威，
      改 db/seed/ci_trend_term.csv / ci_app.csv 后跑 bash db/seed/load_ci_product.sh。

手动跑（不经 Prefect 排期）：
  docker compose exec prefect-worker python flows/ci_demand.py
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timezone

from ci_common import (FetchBlocked, _env, _pg, _robots_allows, _throttle, fetch, is_dry_run,
                       looks_like_bot_wall, maybe_alert)
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

TRENDS_SOURCE = "google_trends"
PLAY_SOURCE = "google_play"
APPSTORE_SOURCE = "app_store"

TRENDS_BASE = "https://trends.google.com/trends/api"
TRENDS_GEO = "DE"
TRENDS_TIMEFRAME = "today 5-y"
TRENDS_MAX_TERMS = 5          # Google Trends 单次请求上限；超了值就不再同一基准


# ===========================================================================
# 一、纯解析函数（不连网、不连库，tests/test_ci_demand.py 覆盖）
# ===========================================================================
def _strip_xssi(text: str) -> str:
    """Google 的 JSON 接口前面带 )]}' 防 XSSI 前缀，从第一个 { 或 [ 开始才是 JSON。"""
    starts = [i for i in (text.find("{"), text.find("[")) if i >= 0]
    if not starts:
        raise ValueError("响应里没有 JSON")
    return text[min(starts):]


def parse_trends_explore(text: str) -> tuple[dict, str]:
    """explore 响应 → 时序 widget 的 (request, token)，供 multiline 接口用。"""
    j = json.loads(_strip_xssi(text))
    for w in j.get("widgets") or []:
        if w.get("id") == "TIMESERIES":
            return w["request"], w["token"]
    raise ValueError("explore 响应里没有 TIMESERIES widget")


def parse_trends_multiline(text: str, terms: list[str]) -> list[dict]:
    """multiline 响应 → 每周 × 每词一行。value 的顺序与请求里的 comparisonItem 顺序一致。"""
    data = json.loads(_strip_xssi(text))["default"]["timelineData"]
    rows = []
    for pt in data:
        start = datetime.fromtimestamp(int(pt["time"]), tz=timezone.utc).date()
        values = pt.get("value") or []
        has = pt.get("hasData") or [None] * len(values)
        if len(values) != len(terms):
            raise ValueError(f"值个数 {len(values)} 与词数 {len(terms)} 不一致")
        for term, v, h in zip(terms, values, has):
            rows.append({"term": term, "period_start": start, "value": int(v),
                         "has_data": h, "is_partial": bool(pt.get("isPartial"))})
    return rows


_AF_RE = re.compile(r"AF_initDataCallback\(\{key: '(ds:\d+)'.*?data:(.*?), sideChannel: \{\}\}\);",
                    re.DOTALL)


def _dig(obj, *path):
    for k in path:
        try:
            obj = obj[k]
        except (IndexError, KeyError, TypeError):
            return None
    return obj


def _installs_min(text: str | None) -> int | None:
    """'1.000.000+' / '1,000,000+' → 1000000。"""
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_play_details(html: str) -> dict:
    """Play 商店详情页 → 指标。数据在内嵌的 ds:5 块里（与 google-play-scraper 的路径一致）。

    路径随 Google 改版可能失效：任何字段取不到就是 None，全空时由调用方按 parse_empty 告警。
    """
    blocks = {m.group(1): m.group(2) for m in _AF_RE.finditer(html)}
    raw = blocks.get("ds:5")
    if not raw:
        return {"title": None, "installs_text": None, "installs_min": None,
                "installs_exact": None, "rating_avg": None, "rating_count": None,
                "review_count": None, "app_version": None, "app_updated_on": None}
    d = _dig(json.loads(raw), 1, 2)
    installs_text = _dig(d, 13, 0)
    updated = _dig(d, 145, 0, 1, 0)
    rating = _dig(d, 51, 0, 1)
    return {
        "title": _dig(d, 0, 0),
        "installs_text": installs_text,
        "installs_min": _dig(d, 13, 1) or _installs_min(installs_text),
        "installs_exact": _dig(d, 13, 2),
        "rating_avg": round(float(rating), 3) if rating is not None else None,
        "rating_count": _dig(d, 51, 2, 1),
        "review_count": _dig(d, 51, 3, 1),
        "app_version": _dig(d, 140, 0, 0, 0),
        "app_updated_on": (datetime.fromtimestamp(int(updated), tz=timezone.utc).date()
                           if isinstance(updated, (int, float)) else None),
    }


def parse_itunes_lookup(payload: dict) -> dict | None:
    res = (payload or {}).get("results") or []
    if not res:
        return None
    r = res[0]
    rel = r.get("currentVersionReleaseDate")
    rating = r.get("averageUserRating")
    return {
        "title": r.get("trackName"),
        "rating_avg": round(float(rating), 3) if rating is not None else None,
        "rating_count": r.get("userRatingCount"),
        "app_version": r.get("version"),
        "app_updated_on": date.fromisoformat(rel[:10]) if rel else None,
    }


# ===========================================================================
# 二、配置与入库
# ===========================================================================
def trend_groups() -> dict[str, list[str]]:
    """line → 搜索词（按 sort_order）。一条线超过 5 个 active 词直接报错，不悄悄截断。"""
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT line, term FROM core.ci_trend_term WHERE active "
                        "ORDER BY line, sort_order, term")
            out: dict[str, list[str]] = {}
            for line, term in cur.fetchall():
                out.setdefault(line, []).append(term)
    for line, terms in out.items():
        if len(terms) > TRENDS_MAX_TERMS:
            raise ValueError(f"情报线 {line} 有 {len(terms)} 个搜索词，超过 Google Trends "
                             f"单次上限 {TRENDS_MAX_TERMS}，值将不可比 —— 改 ci_trend_term.csv")
    return out


def last_trend_fetch(line: str) -> date | None:
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT max(fetched_on) FROM raw.ci_search_trend "
                        "WHERE line=%s AND geo=%s AND timeframe=%s",
                        (line, TRENDS_GEO, TRENDS_TIMEFRAME))
            return cur.fetchone()[0]


def apps() -> list[tuple[str, str, str]]:
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT store, app_id, brand FROM core.ci_app WHERE active ORDER BY 1, 2")
            return cur.fetchall()


def _write_trends(line: str, rows: list[dict], run_id: str) -> int:
    if not rows or is_dry_run():
        return len(rows)
    today = date.today()
    sql = ("INSERT INTO raw.ci_search_trend (fetched_on, line, geo, timeframe, term, period_start, "
           " value, has_data, is_partial, ingestion_run_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           "ON CONFLICT (line, geo, timeframe, term, period_start, fetched_on) DO NOTHING")
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, [(today, line, TRENDS_GEO, TRENDS_TIMEFRAME, r["term"],
                                   r["period_start"], r["value"], r["has_data"], r["is_partial"],
                                   run_id) for r in rows])
        conn.commit()
    return len(rows)


def _write_app_stat(store: str, app_id: str, d: dict, run_id: str) -> int:
    if is_dry_run():
        return 1
    sql = ("INSERT INTO raw.ci_app_stat (store, app_id, observed_on, installs_text, installs_min, "
           " installs_exact, rating_avg, rating_count, review_count, app_version, app_updated_on, "
           " ingestion_run_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           "ON CONFLICT (store, app_id, observed_on) DO NOTHING")
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (store, app_id, date.today(), d.get("installs_text"),
                              d.get("installs_min"), d.get("installs_exact"), d.get("rating_avg"),
                              d.get("rating_count"), d.get("review_count"), d.get("app_version"),
                              d.get("app_updated_on"), run_id))
        conn.commit()
    return 1


# ===========================================================================
# 三、采集器
# ===========================================================================
def _trends_get(session, path: str, params: dict, logger):
    """Trends 接口 GET：robots + 限速；429 退避一次（Google 对无 cookie 的首个请求常回 429）。"""
    url = f"{TRENDS_BASE}/{path}"
    if not _robots_allows(url):
        raise FetchBlocked(f"robots.txt 禁止: {url}")
    for attempt in (1, 2):
        _throttle("trends.google.com")
        r = session.get(url, params=params, timeout=30,
                        headers={"Accept-Language": "de-DE,de;q=0.9"})
        if r.status_code == 429 and attempt == 1:
            logger.info("Trends 429，60 秒后重试一次")
            time.sleep(60)
            continue
        return r
    return r


@task(retries=1, retry_delay_seconds=120)
def collect_trends(run_id: str) -> dict:
    from curl_cffi import requests as creq     # 延迟导入：解析函数的测试不该依赖它
    logger = get_run_logger()
    stats = {"lines": 0, "rows": 0, "skipped_recent": 0, "failed": 0}
    min_days = int(_env("CI_TRENDS_MIN_DAYS") or "6")
    session = creq.Session(impersonate="chrome")
    for line, terms in trend_groups().items():
        last = last_trend_fetch(line)
        if last and (date.today() - last).days < min_days:
            stats["skipped_recent"] += 1
            logger.info("Trends [%s]: 上次抓取 %s，不足 %s 天，跳过", line, last, min_days)
            continue
        req = {"comparisonItem": [{"keyword": t, "geo": TRENDS_GEO, "time": TRENDS_TIMEFRAME}
                                  for t in terms], "category": 0, "property": ""}
        try:
            r = _trends_get(session, "explore", {"hl": "de", "tz": "0", "req": json.dumps(req)}, logger)
            if r.status_code != 200:
                raise RuntimeError(f"explore HTTP {r.status_code}")
            widget_req, token = parse_trends_explore(r.text)
            r = _trends_get(session, "widgetdata/multiline",
                            {"hl": "de", "tz": "0", "req": json.dumps(widget_req), "token": token},
                            logger)
            if r.status_code != 200:
                raise RuntimeError(f"multiline HTTP {r.status_code}")
            rows = parse_trends_multiline(r.text, terms)
        except Exception as e:          # 非官方接口：任何失败都告警，不让曲线悄悄停更
            stats["failed"] += 1
            logger.warning("Trends [%s] 失败: %s", line, e)
            maybe_alert(TRENDS_SOURCE, "fetch_failed",
                        f"情报线 {line} 的 Google Trends 抓取失败: {e}\n"
                        "非官方接口可能改版或限流;搜索热度曲线会停在上一次抓取。", logger)
            continue
        stats["lines"] += 1
        stats["rows"] += _write_trends(line, rows, run_id)
        logger.info("Trends [%s]: %s 词 × %s 周", line, len(terms), len(rows) // max(len(terms), 1))
    return stats


@task(retries=1, retry_delay_seconds=60)
def collect_app_stats(run_id: str, active_sources: set[str]) -> dict:
    logger = get_run_logger()
    stats = {"apps": 0, "empty": 0, "blocked": 0, "failed": 0}
    for store, app_id, brand in apps():
        source = PLAY_SOURCE if store == "google_play" else APPSTORE_SOURCE
        if source not in active_sources:
            continue
        try:
            if store == "google_play":
                url = f"https://play.google.com/store/apps/details?id={app_id}&hl=de&gl=DE"
                status, body, _ = fetch(url, mode="impersonate")
                html = body.decode("utf-8", "replace")
                if looks_like_bot_wall(html, status):
                    stats["blocked"] += 1
                    logger.warning("Play %s 命中反爬墙 (status=%s)", app_id, status)
                    continue
                d = parse_play_details(html) if status == 200 else None
            else:
                status, body, _ = fetch(f"https://itunes.apple.com/lookup?id={app_id}&country=de",
                                        mode="api")
                d = parse_itunes_lookup(json.loads(body)) if status == 200 else None
        except FetchBlocked as e:
            stats["blocked"] += 1
            logger.warning("%s %s 被拦: %s", store, app_id, e)
            continue
        except Exception as e:
            stats["failed"] += 1
            logger.warning("%s %s 失败: %s", store, app_id, e)
            continue
        if not d or d.get("rating_count") is None:
            stats["empty"] += 1
            logger.warning("%s %s (%s) 解析全空 —— 页面/接口可能已改版 (status=%s)",
                           store, app_id, brand, status)
            maybe_alert(source, "parse_empty",
                        f"{store} {app_id} ({brand}) 取不到评分数,商店页结构可能已改版。", logger)
            continue
        stats["apps"] += _write_app_stat(store, app_id, d, run_id)
        logger.info("%s %s (%s): %s", store, app_id, brand,
                    {k: d.get(k) for k in ("installs_text", "installs_exact", "rating_avg",
                                           "rating_count")})
    return stats


@flow(name="ci-demand")
def ci_demand() -> dict:
    from ci_price import _active_sources   # 单一实现(运维开关 core.ci_source.active)
    logger = get_run_logger()
    run_id = str(getattr(flow_run, "id", "") or "")
    if is_dry_run():
        logger.warning("CI_DRY_RUN=true —— 照常抓取解析但不写库")
    active = _active_sources()
    out = {}
    if TRENDS_SOURCE in active:
        out["trends"] = collect_trends(run_id)
    else:
        logger.info("%s 在 core.ci_source 中已停用，跳过", TRENDS_SOURCE)
    out["apps"] = collect_app_stats(run_id, active)
    logger.info("需求层完成: %s", out)
    return out


if __name__ == "__main__":
    ci_demand()

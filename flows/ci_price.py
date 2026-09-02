"""ChannelHub — 竞品情报：价格与商品页指标日频采集。

覆盖 price 层(mydealz / eBay / idealo / Geizhals)与 retail 层
(Amazon / MediaMarkt / Saturn / Otto)，写 raw.ci_offer + raw.ci_listing_stat。

解析策略(刻意不给每站写 CSS 选择器)：
  · JSON-LD 通用抽取 —— 德国电商为 SEO 几乎都在页面里输出 schema.org/Product +
    Offer + AggregateRating。一份实现覆盖 idealo/Geizhals/MMS/Otto，改版存活率
    远高于手写选择器，也不必我去猜每站的 class 名。
  · Amazon 不发 JSON-LD，单独解析(标题/价格/评分/评论数/BSR)。BSR 是全项目
    唯一的销量信号，只在商品页详情表里。
  · mydealz / eBay 走官方 API，无需解析 HTML。

每源独立 try/except：单源被封只计数告警，不影响其余源当天的采集
(沿用 email_backup/parse_sell_through 的 total 计数器 + 末尾 raise 惯例)。
"""

from __future__ import annotations

import json
import re
from datetime import date

from ci_common import (
    FetchBlocked,
    _env,
    _flag,
    _pg,
    fetch,
    is_dry_run,
    load_products,
    looks_like_bot_wall,
    match_product,
    maybe_alert,
    record_unmatched,
    snapshot,
)
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

# 走通用 JSON-LD 抓取的站(与 core.ci_source.source_code 对应)
SCRAPE_SOURCES = ("idealo", "geizhals", "mediamarkt", "saturn", "otto")

# external_id → 商品页 URL。约定：external_id 以 http 开头则直接当完整 URL 用，
# 这样 Otto 这类带 slug 的脏 URL 不必在代码里硬编模板。
URL_TEMPLATES = {
    "idealo":     "https://www.idealo.de/preisvergleich/OffersOfProduct/{eid}.html",
    "geizhals":   "https://geizhals.de/{eid}.html",
    "mediamarkt": "https://www.mediamarkt.de/de/product/-{eid}.html",
    "saturn":     "https://www.saturn.de/de/product/-{eid}.html",
    "amazon_de":  "https://www.amazon.de/dp/{eid}",
}


def _url_for(source_code: str, external_id: str) -> str:
    if external_id.startswith("http"):
        return external_id
    tmpl = URL_TEMPLATES.get(source_code)
    if not tmpl:
        raise ValueError(f"{source_code} 无 URL 模板，external_id 需填完整 URL")
    return tmpl.format(eid=external_id)


def _eur_to_cents(value) -> int | None:
    if value is None:
        return None
    s = str(value).strip().replace(" ", " ")
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return None
    # 德式 1.234,56 → 1234.56；英式 1,234.56 也要吃下
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return round(float(s) * 100)
    except ValueError:
        return None


# ===========================================================================
# 通用 JSON-LD 抽取(idealo / Geizhals / MediaMarkt / Saturn / Otto)
# ===========================================================================
_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


def _iter_ld_nodes(html: str):
    """页面里所有 JSON-LD 节点(含 @graph 展开)。解析失败的块跳过而非炸掉。"""
    for m in _LD_RE.finditer(html):
        try:
            data = json.loads(m.group(1).strip())
        except (ValueError, TypeError):
            continue
        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, list):
                stack.extend(node)
            elif isinstance(node, dict):
                if "@graph" in node:
                    stack.append(node["@graph"])
                yield node


def parse_jsonld_product(html: str) -> dict:
    """→ {name, offers:[{price_cents,currency,availability,merchant}], rating, review_count}

    多个 Offer 全取(比价站一页多商家正是我们要的)；AggregateOffer 的 low/high 也吃。
    """
    out = {"name": None, "offers": [], "rating": None, "review_count": None}
    for node in _iter_ld_nodes(html):
        types = node.get("@type") or []
        types = [types] if isinstance(types, str) else list(types)
        if "Product" not in types:
            continue
        out["name"] = out["name"] or node.get("name")

        agg = node.get("aggregateRating") or {}
        if isinstance(agg, dict):
            # ratingValue 德语站常写 "4,4" —— float() 会直接抛，评分会静默丢失
            rv = str(agg.get("ratingValue") or "").strip().replace(",", ".")
            if rv and out["rating"] is None:
                try:
                    out["rating"] = float(rv)
                except ValueError:
                    pass
            for k in ("reviewCount", "ratingCount"):
                if agg.get(k) and out["review_count"] is None:
                    try:
                        out["review_count"] = int(str(agg[k]).replace(".", "").replace(",", ""))
                    except ValueError:
                        pass

        offers = node.get("offers")
        offers = offers if isinstance(offers, list) else ([offers] if offers else [])
        for off in offers:
            if not isinstance(off, dict):
                continue
            otypes = off.get("@type") or ""
            otypes = [otypes] if isinstance(otypes, str) else list(otypes)
            if "AggregateOffer" in otypes:
                for key in ("lowPrice", "highPrice"):
                    cents = _eur_to_cents(off.get(key))
                    if cents:
                        out["offers"].append({
                            "price_cents": cents,
                            "currency": off.get("priceCurrency") or "EUR",
                            "availability": (off.get("availability") or "").split("/")[-1] or None,
                            "merchant": f"aggregate:{key}",
                        })
                continue
            cents = _eur_to_cents(off.get("price"))
            if cents is None:
                continue
            seller = off.get("seller") or {}
            merchant = seller.get("name") if isinstance(seller, dict) else str(seller)
            out["offers"].append({
                "price_cents": cents,
                "currency": off.get("priceCurrency") or "EUR",
                "availability": (off.get("availability") or "").split("/")[-1] or None,
                "merchant": merchant or None,
            })
    return out


# ===========================================================================
# Geizhals 专用解析：该站**不输出 JSON-LD**，通用路径在这里不成立
# ===========================================================================
# 每条报价是一个 <div class="offer offer--available …" id="offer-index-N">，块内既有
# 展示价 <span class="gh_price">€ 249,90</span>，也有一段商家点击跟踪的内联 JS，
# 里面带机读的 price: '249.9' / merchant: 'alza.de' —— 优先用后者(无需处理德式
# 千分位与货币符号)，取不到再退回展示价。
# 必须**按 offer 块切分后再匹配**：一个块里有多个 data-merchant-name(按钮 + 商家
# logo 链接)，全局匹配会把价格和商家配错对。
# 锚定 id="offer-index-N" 再回溯到它所属的 <div：真实标记里属性之间是**换行**
# (`<div\nclass="offer offer--available offer--odd"\nid="offer-index-0"\n>`)，
# 任何要求「<div 空格 class」的正则都会直接落空。锚点 + 回溯对属性顺序与空白免疫。
_GH_OFFER_ID_RE = re.compile(r'id="offer-index-\d+"')
_GH_JS_PRICE_RE = re.compile(r"price:\s*'([\d.]+)'")
_GH_JS_MERCHANT_RE = re.compile(r"merchant:\s*'([^']+)'")
_GH_DISPLAY_PRICE_RE = re.compile(r'class="gh_price"[^>]*>\s*(?:&euro;|€)?\s*([\d.,]+)')
_GH_MERCHANT_ATTR_RE = re.compile(r'data-merchant-name="([^"]+)"')
_GH_TITLE_PRICE_RE = re.compile(r'ab\s*(?:&euro;|€)(?:&#xa0;|\s|&nbsp;)*([\d.,]+)', re.I)


def parse_geizhals(html: str) -> dict:
    """→ {name, offers:[{price_cents,currency,availability,merchant}], rating, review_count}

    返回结构与 parse_jsonld_product 一致，便于调用方统一处理。
    """
    out = {"name": None, "offers": [], "rating": None, "review_count": None}
    m = re.search(r"<title>(.*?)</title>", html, re.DOTALL)
    if m:
        out["name"] = re.sub(r"\s+", " ", m.group(1)).split(" ab ")[0].strip() or None

    anchors = [mm.start() for mm in _GH_OFFER_ID_RE.finditer(html)]
    starts = []
    for a in anchors:
        d = html.rfind("<div", 0, a)
        starts.append(d if d >= 0 else a)
    for i, st in enumerate(starts):
        block = html[st:starts[i + 1] if i + 1 < len(starts) else st + 20000]
        mp = _GH_JS_PRICE_RE.search(block)
        cents = _eur_to_cents(mp.group(1)) if mp else None
        if cents is None:
            mp = _GH_DISPLAY_PRICE_RE.search(block)
            cents = _eur_to_cents(mp.group(1)) if mp else None
        if cents is None:
            continue
        mm2 = _GH_JS_MERCHANT_RE.search(block) or _GH_MERCHANT_ATTR_RE.search(block)
        head = re.sub(r"\s+", " ", block[:300]).lower()
        out["offers"].append({
            "price_cents": cents,
            "currency": "EUR",
            # offer--available / offer--shortly / … 是该站的到货状态类
            "availability": "InStock" if "offer--available" in head else None,
            "merchant": mm2.group(1) if mm2 else None,
        })

    # 兜底：报价块一条都没解析出来时，至少从标题的「ab € 249,90」拿到最低价，
    # 好让价格曲线不至于整天断掉(标注 merchant 为 aggregate 以示口径不同)。
    if not out["offers"]:
        mt = _GH_TITLE_PRICE_RE.search(html)
        if mt:
            cents = _eur_to_cents(mt.group(1))
            if cents:
                out["offers"].append({"price_cents": cents, "currency": "EUR",
                                      "availability": None, "merchant": "aggregate:title"})
    return out


# ===========================================================================
# Amazon 专用解析：BSR 是全项目唯一的销量信号
# ===========================================================================
_AMZ_PRICE_RE = re.compile(r'class="a-offscreen">\s*([0-9.,]+)\s*&euro;|class="a-offscreen">\s*&euro;?\s*([0-9.,]+)')
_AMZ_TITLE_RE = re.compile(r'id="productTitle"[^>]*>(.*?)<', re.DOTALL)
_AMZ_RATING_RE = re.compile(r'([0-9],[0-9])\s*von\s*5\s*Sternen', re.IGNORECASE)
_AMZ_REVIEWS_RE = re.compile(r'([\d.,]+)\s*(?:Sternebewertungen|Bewertungen|Rezensionen)', re.IGNORECASE)
# 「Amazon Bestseller-Rang: Nr. 1.234 in Baumarkt」
_AMZ_BSR_RE = re.compile(
    r'Bestseller[- ]Rang[^#]*?Nr\.\s*([\d.,]+)\s*in\s*([^<(&]{2,60})', re.IGNORECASE | re.DOTALL)


def parse_amazon(html: str) -> dict:
    def _int(s):
        try:
            return int(re.sub(r"[^\d]", "", s))
        except (ValueError, TypeError):
            return None

    title = None
    m = _AMZ_TITLE_RE.search(html)
    if m:
        title = re.sub(r"\s+", " ", m.group(1)).strip()

    price_cents = None
    m = _AMZ_PRICE_RE.search(html)
    if m:
        price_cents = _eur_to_cents(m.group(1) or m.group(2))

    rating = None
    m = _AMZ_RATING_RE.search(html)
    if m:
        rating = float(m.group(1).replace(",", "."))

    reviews = None
    m = _AMZ_REVIEWS_RE.search(html)
    if m:
        reviews = _int(m.group(1))

    bsr_rank = bsr_cat = None
    m = _AMZ_BSR_RE.search(html)
    if m:
        bsr_rank = _int(m.group(1))
        bsr_cat = re.sub(r"\s+", " ", m.group(2)).strip()

    return {"name": title, "price_cents": price_cents, "rating": rating,
            "review_count": reviews, "bsr_rank": bsr_rank, "bsr_category": bsr_cat}


# ===========================================================================
# 入库(append-only，日频幂等)
# ===========================================================================
def _write_offers(rows) -> int:
    if not rows:
        return 0
    if is_dry_run():
        return len(rows)          # 干跑报「本该写入多少条」，否则这轮验证等于没验
    sql = ("INSERT INTO raw.ci_offer (product_id, source_code, merchant_name, price_cents, "
           " currency, shipping_cents, availability, offer_url, observed_on, snapshot_id, "
           " ingestion_run_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           "ON CONFLICT (source_code, product_id, merchant_name, observed_on) DO NOTHING")
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    return len(rows)


def _write_stat(row) -> int:
    if not row:
        return 0
    if is_dry_run():
        return 1                  # 同上：干跑也要能看出解析确实产出了数据
    sql = ("INSERT INTO raw.ci_listing_stat (product_id, source_code, rating_avg, rating_count, "
           " review_count, bsr_category, bsr_rank, price_cents, in_stock, observed_on, "
           " snapshot_id, ingestion_run_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
           "ON CONFLICT (source_code, product_id, observed_on) DO NOTHING")
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, row)
        conn.commit()
    return 1


def _active_sources() -> set[str]:
    """core.ci_source.active 是**运维开关**：停采某个源改数据即可，不必改代码。

    迁移里不写死它 —— 迁移每次 deploy 重放，写死会把人工的启停覆盖掉。
    """
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_code FROM core.ci_source WHERE active")
            return {r[0] for r in cur.fetchall()}


def _aliases(source_code: str) -> list[tuple[str, str]]:
    """(product_id, external_id) —— 靠稳定 id 直接取数，不靠标题去猜。"""
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT a.product_id, a.external_id FROM core.ci_product_alias a "
                "JOIN core.ci_product p ON p.product_id = a.product_id AND p.active "
                "WHERE a.source_code = %s ORDER BY a.product_id",
                (source_code,),
            )
            return cur.fetchall()


# ===========================================================================
# 各源采集器
# ===========================================================================
@task(retries=2, retry_delay_seconds=30)
def collect_page_source(source_code: str, run_id: str) -> dict:
    """JSON-LD 通用路径 + Amazon 专用路径，均按 alias 里的稳定 id 逐个取页。"""
    logger = get_run_logger()
    stats = {"pages": 0, "offers": 0, "stats": 0, "blocked": 0, "empty": 0}
    today = date.today()
    pairs = _aliases(source_code)
    if not pairs:
        logger.info("%s: 无 alias，跳过(在 db/seed/ci_product_alias.csv 补标识)", source_code)
        return stats

    mode = "impersonate" if source_code in ("idealo", "geizhals", "amazon_de", "otto") else "http"
    for product_id, external_id in pairs:
        url = _url_for(source_code, external_id)
        try:
            status, body, ctype = fetch(url, mode=mode)
        except FetchBlocked as e:
            stats["blocked"] += 1
            logger.warning("%s %s 被拦: %s", source_code, product_id, e)
            continue
        html = body.decode("utf-8", "replace")
        if looks_like_bot_wall(html, status):
            # 关键：反爬墙必须显式失败，不能静默存一条空记录当作「今天没数据」
            stats["blocked"] += 1
            logger.warning("%s %s 命中反爬墙 (status=%s)", source_code, product_id, status)
            continue

        stats["pages"] += 1
        snap = snapshot(source_code, url, status, body, ctype,
                        product_id=product_id, run_id=run_id)

        if source_code == "amazon_de":
            d = parse_amazon(html)
            if d["price_cents"] is None and d["review_count"] is None and d["bsr_rank"] is None:
                stats["empty"] += 1
                logger.warning("%s %s 解析全空 —— 页面结构可能已改版", source_code, product_id)
                continue
            stats["offers"] += _write_offers([(
                product_id, source_code, "Amazon", d["price_cents"], "EUR", None,
                None, url, today, snap, run_id)]) if d["price_cents"] else 0
            stats["stats"] += _write_stat((
                product_id, source_code, d["rating"], None, d["review_count"],
                d["bsr_category"], d["bsr_rank"], d["price_cents"],
                d["price_cents"] is not None, today, snap, run_id))
            continue

        # Geizhals 不发 JSON-LD，走专用解析器；其余站走通用 JSON-LD 路径
        d = parse_geizhals(html) if source_code == "geizhals" else parse_jsonld_product(html)
        if not d["offers"] and d["review_count"] is None:
            stats["empty"] += 1
            logger.warning("%s %s 解析全空 —— 该站可能改版或需补专用解析",
                           source_code, product_id)
            continue
        rows, seen = [], set()
        for off in d["offers"]:
            merchant = (off["merchant"] or source_code)[:200]
            if merchant in seen:            # 同商家一天只留一条(唯一键也会兜住)
                continue
            seen.add(merchant)
            rows.append((product_id, source_code, merchant, off["price_cents"],
                         off["currency"], None, off["availability"], url, today, snap, run_id))
        stats["offers"] += _write_offers(rows)
        best = min((o["price_cents"] for o in d["offers"]), default=None)
        stats["stats"] += _write_stat((
            product_id, source_code, d["rating"], None, d["review_count"],
            None, None, best, bool(d["offers"]), today, snap, run_id))
    return stats


# ---------------------------------------------------------------------------
# mydealz：走**公开 RSS 分组 feed**，不走需签名的 REST
# ---------------------------------------------------------------------------
# Pepper 的 /rest_api/v2 需要应用签名(不带签名一律 401 signature_missing_paramter)，
# 我们没有凭据。但站点同时提供**无需鉴权的 RSS**：/rss/gruppe/<slug> 每条 item 里
# 直接带 <pepper:merchant name="…" price="…"/> —— 商家与价格都有，正是我们要的。
# 尤其 /rss/gruppe/ecovacs 是**品牌分组**，天然只出竞品的帖子。
# robots.txt 对 User-agent:* 允许 /rss/ 与 /deals/(该站另行单独禁止了一批 AI 训练
# 爬虫;我们不是那类 —— 用途是比价监控，不是建训练语料)。
MYDEALZ_DEFAULT_GROUPS = ("ecovacs", "saugroboter", "haushaltsgeraete")


def mydealz_groups() -> list[str]:
    raw = _env("CI_MYDEALZ_GROUPS")
    return [g.strip() for g in raw.split(",") if g.strip()] or list(MYDEALZ_DEFAULT_GROUPS)


def parse_mydealz_rss(xml_bytes: bytes) -> list[dict]:
    """mydealz RSS → [{title, link, guid, published, merchant, price_cents}]

    价格与商家在自定义命名空间元素 <pepper:merchant name=… price=…/> 上;
    ElementTree 会把标签展开成 {uri}merchant，故按去掉命名空间后的名字匹配。
    """
    import xml.etree.ElementTree as ET
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    out = []
    for it in root.iter():
        if it.tag.split("}")[-1] != "item":
            continue
        d = {"title": None, "link": None, "guid": None, "published": None,
             "merchant": None, "price_cents": None}
        for ch in it:
            tag = ch.tag.split("}")[-1]
            if tag == "title":
                d["title"] = (ch.text or "").strip()
            elif tag == "link":
                d["link"] = (ch.text or "").strip()
            elif tag == "guid":
                d["guid"] = (ch.text or "").strip()
            elif tag == "pubDate":
                d["published"] = (ch.text or "").strip()
            elif tag == "merchant":
                d["merchant"] = (ch.attrib.get("name") or "").strip() or None
                d["price_cents"] = _eur_to_cents(ch.attrib.get("price"))
        if d["title"] and d["link"]:
            out.append(d)
    return out


@task(retries=2, retry_delay_seconds=30)
def collect_mydealz(run_id: str) -> dict:
    """mydealz 公开 RSS 分组 feed → raw.ci_offer（促销价，非每日必有）。"""
    logger = get_run_logger()
    stats = {"pages": 0, "offers": 0, "unmatched": 0, "items": 0}
    today = date.today()
    products = load_products()
    rows, seen = [], set()

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
        snapshot("mydealz", url, status, body, ctype, run_id=run_id)

        items = parse_mydealz_rss(body)
        stats["items"] += len(items)
        for it in items:
            # 一条 deal 就是一个具体商品，属单品语境:歧义必须判失败，不猜
            pid, reason = match_product(it["title"], products)
            if pid is None:
                if reason not in ("accessory", "no_match"):
                    stats["unmatched"] += 1
                    record_unmatched("mydealz", it["guid"] or it["link"],
                                     it["title"], it["link"])
                continue
            if it["price_cents"] is None:
                continue
            merchant = (it["merchant"] or "mydealz")[:200]
            key = (pid, merchant)
            if key in seen:            # 同商家同天只留一条(唯一键也会兜住)
                continue
            seen.add(key)
            rows.append((pid, "mydealz", merchant, it["price_cents"], "EUR", None,
                         None, it["link"], today, None, run_id))
    stats["offers"] = _write_offers(rows)
    if stats["pages"] and not stats["items"]:
        logger.warning("mydealz 取到 feed 但一条 item 都没解析出 —— feed 结构可能已改")
    return stats


@task(retries=2, retry_delay_seconds=30)
def collect_ebay(run_id: str) -> dict:
    """eBay 官方 Browse API(OAuth client-credentials)。缺凭证则整源跳过。"""
    logger = get_run_logger()
    stats = {"pages": 0, "offers": 0, "unmatched": 0}
    cid, csec = _env("EBAY_CLIENT_ID"), _env("EBAY_CLIENT_SECRET")
    if not (cid and csec):
        logger.info("eBay: 未配置 EBAY_CLIENT_ID/SECRET，跳过")
        return stats

    import base64

    import httpx
    tok = httpx.post(
        "https://api.ebay.com/identity/v1/oauth2/token",
        headers={"Authorization": "Basic " + base64.b64encode(f"{cid}:{csec}".encode()).decode(),
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials",
              "scope": "https://api.ebay.com/oauth/api_scope"}, timeout=30)
    tok.raise_for_status()
    access = tok.json()["access_token"]

    today = date.today()
    products = load_products()
    for p in products:
        r = httpx.get("https://api.ebay.com/buy/browse/v1/item_summary/search",
                      headers={"Authorization": f"Bearer {access}",
                               "X-EBAY-C-MARKETPLACE-ID": "EBAY_DE"},
                      params={"q": p.display_name, "limit": 50}, timeout=30)
        stats["pages"] += 1
        if r.status_code != 200:
            logger.warning("eBay %s HTTP %s", p.product_id, r.status_code)
            continue
        rows = []
        for it in r.json().get("itemSummaries", []) or []:
            title = it.get("title") or ""
            matched, _ = match_product(title, products)
            if matched != p.product_id:
                if matched is None:
                    stats["unmatched"] += 1
                    record_unmatched("ebay", it.get("itemId", title)[:200], title, it.get("itemWebUrl", ""))
                continue
            cents = _eur_to_cents((it.get("price") or {}).get("value"))
            if cents is None:
                continue
            rows.append((p.product_id, "ebay", (it.get("seller") or {}).get("username", "ebay")[:200],
                         cents, (it.get("price") or {}).get("currency", "EUR"), None,
                         None, it.get("itemWebUrl"), today, None, run_id))
        stats["offers"] += _write_offers(rows)
    return stats


# ===========================================================================
# 编排
# ===========================================================================
@flow(name="ci-price")
def ci_price() -> dict:
    logger = get_run_logger()
    run_id = str(getattr(flow_run, "id", "") or "")
    total = {"sources": 0, "failed": 0, "offers": 0, "stats": 0, "blocked": 0,
             "unmatched": 0, "auth_error": 0}
    if is_dry_run():
        logger.warning("CI_DRY_RUN=true —— 照常抓取解析但不写库、不存 MinIO")

    jobs = [("mydealz", lambda: collect_mydealz(run_id)),
            ("ebay", lambda: collect_ebay(run_id))]
    if _flag("CI_AMAZON_ENABLED", "true"):
        jobs.append(("amazon_de", lambda: collect_page_source("amazon_de", run_id)))
    else:
        logger.info("CI_AMAZON_ENABLED=false —— 跳过 Amazon")
    for sc in SCRAPE_SOURCES:
        jobs.append((sc, lambda s=sc: collect_page_source(s, run_id)))

    active = _active_sources()
    for source_code, job in jobs:
        if source_code not in active:
            # 停采的源不该每天失败一次、再发一封告警邮件
            logger.info("%s 在 core.ci_source 中已停用，跳过", source_code)
            continue
        total["sources"] += 1
        try:
            st = job()
            for k in ("offers", "stats", "blocked", "unmatched"):
                total[k] += st.get(k, 0)
            total["auth_error"] += st.get("auth_error", 0)
            logger.info("%s 完成: %s", source_code, st)
            if st.get("blocked"):
                maybe_alert(source_code, "bot_wall",
                            f"{st['blocked']} 个页面命中反爬墙或被 robots/白名单拦下。", logger)
            if st.get("auth_error"):
                maybe_alert(source_code, "api_auth_required",
                            "接口要求鉴权/签名，该源当前不可用；0 条结果不代表市场上没有。",
                            logger)
            if st.get("empty"):
                maybe_alert(source_code, "parse_empty",
                            f"{st['empty']} 个页面取到但解析全空 —— 多半是页面结构改版。", logger)
        except Exception as e:                      # 单源失败不拖累其余源
            total["failed"] += 1
            logger.warning("%s 失败: %s", source_code, e, exc_info=True)
            try:
                maybe_alert(source_code, "collect_failed", f"{type(e).__name__}: {e}", logger)
            except Exception:
                logger.warning("%s 告警本身也失败了", source_code)

    logger.info("ci-price 汇总: %s", total)
    if total["failed"]:
        # 已入库的观测保留，Prefect UI 变红提示有源需要处理(沿用仓库惯例)
        raise RuntimeError(f"{total['failed']} 个源采集失败: {total}")
    return total


if __name__ == "__main__":
    ci_price()

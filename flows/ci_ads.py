"""ChannelHub — 竞品情报：广告层采集（周频）。

两个源，全部写 raw.ci_ad（状态型 upsert，理由见 db/migrations/019_ci_ads.sql 文件头）：

  · Meta   Graph API /ads_archive（Ad Library API）
           欧盟触达过的广告**不限类型**都能查（DSA），并带 eu_total_reach、定向年龄/
           性别/地区、受益方与付款方。准入：developers.facebook.com 建应用 + 在
           facebook.com/ID 完成身份确认，拿一个 user access token。
  · Google BigQuery 公共数据集 bigquery-public-data.google_ads_transparency_center
           .creative_stats —— Google 对商业广告**唯一**的官方批量入口（仅 EEA）。
           有首末展示日、展示次数区间、投放面（YouTube/Search/…）、定向方式；
           **没有广告文案**，文案只能点 ad_url 去透明度中心看。
           准入：一个开了 BigQuery 的 GCP 项目 + service account（查询费记在该项目，
           每月前 1 TiB 免费）。

检索策略（两源相同）：
  core.ci_ad_advertiser 里给某品牌登记了广告主 id → 精确按 id 取；
  没登记 → 退回按品牌名检索，再用 core.ci_product.brand_regex 过「广告主名」，
  排掉转售商和同名公司。不知道 id 时先跑发现命令（见文件末尾 / 文档「广告层」）。
  Google 另有一条：core.ci_ad_domain 给某品牌登记了落地页域名 → 每轮先去透明度中心
  按域名取素材 id，再按素材 id 取。用于走代投代理**共用账户**的品牌（imoo 挂在
  BlueVision 的账户下，该账户 6.7 万条素材只有 93 条是 imoo），理由见 021 文件头。

!! 两处硬约束 !!
  1) Meta 的 ad_snapshot_url 与分页 paging.next 都**内嵌 access_token**。前者根本不请求，
     改用公开的 facebook.com/ads/library/?id=；后者只在内存里跟随，绝不入库、绝不打日志。
     这也是本 flow 不调用 snapshot() 存 MinIO 的原因之一：原样存档等于泄漏 token。
     （另一个原因：两个源都是结构化官方数据，可按 id 随时重取，不需要回溯重解析。）
  2) BigQuery 按扫描量计费，creative_stats 约 150 GB。每次真查询前先 dryRun（免费）
     拿 totalBytesProcessed，超过 CI_GOOGLE_ADS_MAX_GB 就跳过并告警 cost_guard，
     绝不让一条改坏的 SQL 悄悄烧钱。

Google 表结构不写死：网上的示例字段名互相矛盾（creative_id / ad_id），且 Google 可能改表。
每次运行先 tables.get 读真实 schema，按候选名解析列；缺必需列就告警 schema_changed
并跳过 Google，不猜。
"""

from __future__ import annotations

import base64
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta

from ci_common import _env, _pg, is_dry_run, load_products, match_products_all, maybe_alert
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

META_SOURCE = "meta_ads"
GOOGLE_SOURCE = "google_ads"

BQ_TABLE = "bigquery-public-data.google_ads_transparency_center.creative_stats"
BQ_TABLE_API = ("projects/bigquery-public-data/datasets/google_ads_transparency_center"
                "/tables/creative_stats")
BQ_BASE = "https://bigquery.googleapis.com/bigquery/v2"
BQ_SCOPE = "https://www.googleapis.com/auth/bigquery"

META_FIELDS = ",".join((
    "id", "ad_creation_time", "ad_delivery_start_time", "ad_delivery_stop_time",
    "ad_creative_bodies", "ad_creative_link_titles", "ad_creative_link_descriptions",
    "page_id", "page_name", "publisher_platforms", "languages",
    "eu_total_reach", "target_ages", "target_gender", "target_locations",
    "beneficiary_payers",
    # 刻意不要 ad_snapshot_url：URL 里带 access_token，见文件头硬约束 1
))


def _log():
    """flow 内用 Prefect logger；discover 命令在 flow 外跑，退回标准 logging。"""
    try:
        return get_run_logger()
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        return logging.getLogger("ci_ads")


def _region() -> str:
    return (_env("CI_ADS_REGION") or "DE").upper()


def _lookback_days() -> int:
    return int(_env("CI_ADS_LOOKBACK_DAYS") or "365")


def _to_date(v) -> date | None:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


# ===========================================================================
# 一、检索目标：品牌 × 源
# ===========================================================================
def brand_targets(products) -> dict[str, re.Pattern]:
    """品牌 → 判定广告主名的正则。取 core.ci_product.brand_regex；缺省按品牌名词边界。"""
    out: dict[str, re.Pattern] = {}
    for p in products:
        if p.brand in out:
            continue
        out[p.brand] = p.brand_re or re.compile(rf"\b{re.escape(p.brand)}\b", re.IGNORECASE)
    return out


OPT_OUT = "*"      # advertiser_id='*' 且 active=false：该品牌在该源**不按名检索**


def advertiser_config() -> tuple[dict, dict]:
    """core.ci_ad_advertiser → (allow, deny)，键都是 (source_code, brand)。

      active=true   白名单：该品牌在该源改为**只按这些 id 精确取**
      active=false  黑名单：按名检索时排除这些 id（发现命令里的同名公司）
      '*' + false   禁用按名检索：品牌名太常见、按名只会搜到同名公司时用
                    （实测 HUTT 在 Google 上按名搜到 14 个广告主，全是画廊、牙科、T 恤店）
    """
    allow: dict[tuple[str, str], list[str]] = {}
    deny: dict[tuple[str, str], set[str]] = {}
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_code, brand, advertiser_id, active "
                        "FROM core.ci_ad_advertiser ORDER BY 1, 2, 3")
            for src, brand, aid, active in cur.fetchall():
                if active:
                    allow.setdefault((src, brand), []).append(aid)
                else:
                    deny.setdefault((src, brand), set()).add(aid)
    return allow, deny


def _prune(source_code: str, brands, allow: dict, deny: dict,
           domain_cr: dict[str, set[str]] | None = None) -> int:
    """配置收窄后清掉旧行 —— raw.ci_ad 是状态型 upsert，不清就永远留着。

    删三类：① 被显式排除的广告主；② 白名单 / 禁用按名检索的品牌下，不在白名单里的广告主
    （多半是之前按名检索误入的同名公司）；③ 登记了域名的品牌下，既不在白名单账户、
    也不在本轮域名素材清单里的广告。只在该源本轮采集成功后调用。
    domain_cr 只放**本轮域名检索全部成功**的品牌 —— 检索失败时不能据此删行。
    """
    if is_dry_run():
        return 0
    domain_cr = domain_cr or {}
    # 登记了域名的品牌不走 ② —— 它的素材可以挂在白名单以外的账户下（代理共用账户）
    restricted = [b for b in brands if b not in domain_cr and (
        allow.get((source_code, b)) or OPT_OUT in deny.get((source_code, b), set()))]
    allowed = [a for (src, _), ids in allow.items() if src == source_code for a in ids]
    excluded = [a for (src, _), ids in deny.items() if src == source_code
                for a in ids if a != OPT_OUT]
    if not restricted and not excluded and not domain_cr:
        return 0
    n = 0
    with _pg() as conn:
        with conn.cursor() as cur:
            if restricted or excluded:
                cur.execute(
                    "DELETE FROM raw.ci_ad WHERE source_code = %s AND ("
                    "  (brand = ANY(%s) AND NOT advertiser_id = ANY(%s))"
                    "  OR advertiser_id = ANY(%s))",
                    (source_code, restricted, allowed, excluded))
                n += cur.rowcount
            for brand, cids in domain_cr.items():
                cur.execute(
                    "DELETE FROM raw.ci_ad WHERE source_code = %s AND brand = %s"
                    "  AND NOT advertiser_id = ANY(%s) AND NOT ad_id = ANY(%s)",
                    (source_code, brand, allow.get((source_code, brand), []), sorted(cids)))
                n += cur.rowcount
        conn.commit()
    return n


# ===========================================================================
# 二、写库
# ===========================================================================
AD_COLS = ("source_code", "ad_id", "advertiser_id", "advertiser_name", "brand", "ad_format",
           "surfaces", "title", "body", "languages", "first_shown", "last_shown",
           "is_active", "exposure_lower", "exposure_upper", "exposure_kind", "targeting",
           "product_ids", "ad_url", "ingestion_run_id")


def _write_ads(rows: list[dict]) -> int:
    """状态型 upsert：除 first_seen_at 外全部刷新（源给的是生命周期累计值）。"""
    uniq: dict[tuple[str, str], dict] = {}
    for r in rows:
        uniq.setdefault((r["source_code"], r["ad_id"]), r)
    rows = list(uniq.values())
    if not rows:
        return 0
    if is_dry_run():
        return len(rows)          # 干跑报「本该写入多少条」
    placeholders = ", ".join("%s::jsonb" if c == "targeting" else "%s" for c in AD_COLS)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in AD_COLS
                        if c not in ("source_code", "ad_id"))
    sql = (f"INSERT INTO raw.ci_ad ({', '.join(AD_COLS)}) VALUES ({placeholders}) "
           f"ON CONFLICT (source_code, ad_id) DO UPDATE SET {updates}, last_seen_at = now()")
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, [
                tuple(json.dumps(r[c] or {}, ensure_ascii=False) if c == "targeting" else r[c]
                      for c in AD_COLS)
                for r in rows])
        conn.commit()
    return len(rows)


# ===========================================================================
# 三、Meta Ad Library
# ===========================================================================
def meta_error_kind(err: dict) -> str:
    """Graph 错误分诊：处置完全不同，必须分开（同 ci_social._ig_get 的理由）。"""
    code = err.get("code")
    if code == 190:
        return "auth_expired"                    # token 失效/过期 —— 换 token
    if code in (4, 17, 32, 613, 80004):
        return "rate_limited"                    # 撞限流 —— 下轮再来
    if code == 10 or (isinstance(code, int) and 200 <= code <= 299):
        return "permission"                      # 没做身份确认 / 应用没这个权限
    return "error"


def parse_meta_ad(ad: dict, brand: str, products, run_id: str,
                  today: date | None = None) -> dict:
    today = today or date.today()
    titles = [t for t in (ad.get("ad_creative_link_titles") or []) if t]
    bodies = list(dict.fromkeys(b for b in (ad.get("ad_creative_bodies") or []) if b))
    descs = [d for d in (ad.get("ad_creative_link_descriptions") or []) if d]
    title = titles[0] if titles else None
    body = "\n\n".join(bodies) or None
    start = _to_date(ad.get("ad_delivery_start_time")) or _to_date(ad.get("ad_creation_time"))
    stop = _to_date(ad.get("ad_delivery_stop_time"))
    running = stop is None or stop >= today
    reach = ad.get("eu_total_reach")
    text = "\n".join(filter(None, [title, body, *descs]))
    targeting = {k: v for k, v in {
        "ages": ad.get("target_ages"),
        "gender": ad.get("target_gender"),
        "locations": ad.get("target_locations"),
        "beneficiary_payers": ad.get("beneficiary_payers"),
    }.items() if v}
    return {
        "source_code": META_SOURCE,
        "ad_id": str(ad["id"]),
        "advertiser_id": str(ad.get("page_id") or ""),
        "advertiser_name": ad.get("page_name"),
        "brand": brand,
        "ad_format": None,
        "surfaces": [str(p).lower() for p in (ad.get("publisher_platforms") or [])],
        "title": (title or "")[:1000] or None,
        "body": body,
        "languages": list(ad.get("languages") or []),
        "first_shown": start,
        "last_shown": today if running else stop,
        "is_active": running,
        "exposure_lower": reach,
        "exposure_upper": reach,
        "exposure_kind": "reach" if reach is not None else None,
        "targeting": targeting,
        "product_ids": match_products_all(text, products) if text else [],
        # 公开详情页；**不是** ad_snapshot_url（那个带 token）
        "ad_url": f"https://www.facebook.com/ads/library/?id={ad['id']}",
        "ingestion_run_id": run_id,
    }


def _meta_pages(params: dict, logger, max_pages: int):
    """跟随分页逐页产出 (kind, data)。kind='ok' 时 data 是广告列表，否则是错误 dict。

    paging.next 里带 access_token —— 只在这个函数的局部变量里流转，不记日志。
    """
    import httpx
    version = _env("META_GRAPH_VERSION") or "v26.0"
    url = f"https://graph.facebook.com/{version}/ads_archive"
    req_params: dict | None = params
    for _ in range(max_pages):
        r = httpx.get(url, params=req_params, timeout=60)
        try:
            j = r.json()
        except ValueError:
            j = {}
        if r.status_code != 200 or "error" in j:
            yield "error", (j.get("error") or {"code": None, "message": f"HTTP {r.status_code}"})
            return
        yield "ok", j.get("data") or []
        nxt = (j.get("paging") or {}).get("next")
        if not nxt:
            return
        url, req_params = nxt, None           # next 已含全部参数（包括 token）


@task(retries=1, retry_delay_seconds=60)
def collect_meta_ads(run_id: str) -> dict:
    logger = _log()
    stats = {"queries": 0, "ads": 0, "other_advertiser": 0, "auth_error": 0,
             "rate_limited": 0}
    token = _env("META_AD_LIBRARY_TOKEN")
    if not token:
        logger.info("Meta: 未配置 META_AD_LIBRARY_TOKEN，跳过")
        return stats
    products = load_products()
    brands = brand_targets(products)
    allow, deny = advertiser_config()
    region = _region()
    since = (date.today() - timedelta(days=_lookback_days())).isoformat()
    max_pages = int(_env("CI_ADS_MAX_PAGES") or "20")
    base = {"access_token": token, "ad_reached_countries": json.dumps([region]),
            "ad_type": "ALL", "ad_active_status": "ALL", "ad_delivery_date_min": since,
            "fields": META_FIELDS, "limit": 100}
    rows: list[dict] = []
    stats["pruned"] = 0
    for brand, brand_re in brands.items():
        ids = allow.get((META_SOURCE, brand))
        blocked = deny.get((META_SOURCE, brand), set())
        if not ids and OPT_OUT in blocked:
            logger.info("Meta [%s]: 已禁用按名检索且无白名单 id，跳过", brand)
            continue
        if ids:
            # 按主页 id 精确取：一次最多 10 个 id（API 限制）
            batches = [{"search_page_ids": json.dumps(ids[i:i + 10])}
                       for i in range(0, len(ids), 10)]
        else:
            batches = [{"search_terms": brand}]
        for extra in batches:
            stats["queries"] += 1
            for kind, data in _meta_pages({**base, **extra}, logger, max_pages):
                if kind == "error":
                    ek = meta_error_kind(data)
                    logger.warning("Meta [%s] %s: code=%s %s", brand, ek, data.get("code"),
                                   str(data.get("message"))[:200])
                    if ek in ("auth_expired", "permission"):
                        stats["auth_error"] += 1
                        stats["ads"] = _write_ads(rows)
                        return stats              # token/权限问题，换品牌也一样，直接收手
                    if ek == "rate_limited":
                        stats["rate_limited"] += 1
                    break
                for ad in data:
                    # 关键词模式下结果按**文案**命中，转售商/同名公司也会进来 —— 用主页名把关
                    if not ids and (not brand_re.search(ad.get("page_name") or "")
                                    or str(ad.get("page_id")) in blocked):
                        stats["other_advertiser"] += 1
                        continue
                    rows.append(parse_meta_ad(ad, brand, products, run_id))
    stats["ads"] = _write_ads(rows)
    stats["pruned"] = _prune(META_SOURCE, brands, allow, deny)
    return stats


# ===========================================================================
# 四、Google Ads Transparency Center（BigQuery 公共数据集）
# ===========================================================================
class SchemaChanged(RuntimeError):
    """creative_stats 缺少必需列 —— Google 改表了，需要人看，不猜。"""


class CostGuard(RuntimeError):
    """dryRun 预估扫描量超上限。"""


# 逻辑字段 → 候选路径（按优先级）。region_stats 下的路径在 UNNEST 后用别名 r 引用。
REQUIRED_COLS = {
    "advertiser_id": ["advertiser_id"],
    "creative_id": ["creative_id", "ad_id"],
    "advertiser_name": ["advertiser_disclosed_name", "advertiser_legal_name", "advertiser_name"],
    "region_code": ["region_stats.region_code"],
}
OPTIONAL_COLS = {
    "ad_format": ["ad_format_type", "ad_format"],
    "page_url": ["creative_page_url"],
    "first_shown": ["region_stats.first_shown", "region_stats.first_shown_date"],
    "last_shown": ["region_stats.last_shown", "region_stats.last_shown_date"],
    "shown_lower": ["region_stats.times_shown_lower_bound"],
    "shown_upper": ["region_stats.times_shown_upper_bound"],
    # 实测(2026-09)真实字段名是 .surface;surface_code 是早期资料里的写法,留作候选
    "surface_code": ["region_stats.surface_serving_stats.surface_serving_stats.surface",
                     "region_stats.surface_serving_stats.surface_serving_stats.surface_code",
                     "region_stats.surface_serving_stats.surface_code"],
    "audience": ["audience_selection_approach_info"],
}


def flatten_schema(fields: list[dict], prefix: str = "") -> dict[str, dict]:
    """BigQuery schema → {点分路径: {type, mode}}，嵌套 RECORD 逐层展开。"""
    out: dict[str, dict] = {}
    for f in fields:
        path = f"{prefix}{f['name']}"
        out[path] = {"type": f.get("type"), "mode": f.get("mode") or "NULLABLE"}
        if f.get("fields"):
            out.update(flatten_schema(f["fields"], path + "."))
    return out


def resolve_columns(schema: dict[str, dict]) -> dict[str, str | None]:
    cols: dict[str, str | None] = {}
    missing = []
    for key, cands in REQUIRED_COLS.items():
        hit = next((c for c in cands if c in schema), None)
        if not hit:
            missing.append(f"{key}（候选 {'/'.join(cands)}）")
        cols[key] = hit
    if schema.get("region_stats", {}).get("mode") != "REPEATED":
        missing.append("region_stats 不是 REPEATED 数组")
    if missing:
        raise SchemaChanged("creative_stats 缺少必需列: " + "; ".join(missing))
    for key, cands in OPTIONAL_COLS.items():
        cols[key] = next((c for c in cands if c in schema), None)
    return cols


def _col_expr(path: str) -> str:
    """顶层列用 c.，region_stats 下的列用 UNNEST 别名 r.（中间不得再有 REPEATED）。"""
    if path.startswith("region_stats."):
        return "r." + path[len("region_stats."):]
    return "c." + path


def _surface_expr(path: str | None, schema: dict[str, dict]) -> str:
    """投放面数组表达式。路径上恰好一个 REPEATED 段时 UNNEST 它；其它形状放弃（返回空数组）。"""
    empty = "CAST([] AS ARRAY<STRING>)"
    if not path or not path.startswith("region_stats."):
        return empty
    segs = path.split(".")[1:]                   # 去掉 region_stats
    repeated = [i for i in range(len(segs) - 1)
                if schema.get("region_stats." + ".".join(segs[:i + 1]), {}).get("mode")
                == "REPEATED"]
    if not repeated:
        return f"[SAFE_CAST(r.{'.'.join(segs)} AS STRING)]"
    if len(repeated) > 1:
        return empty
    i = repeated[0]
    arr = "r." + ".".join(segs[:i + 1])
    leaf = ".".join(segs[i + 1:])
    return f"ARRAY(SELECT SAFE_CAST(s.{leaf} AS STRING) FROM UNNEST({arr}) AS s)"


def build_google_sql(cols: dict[str, str | None], schema: dict[str, dict],
                     brand_specs: list[tuple]) -> tuple[str, list[dict]]:
    """brand_specs: [(brand, 'ids', [ids]) | (brand, 'regex', 正则串, [排除 ids])]
       → (SQL, queryParameters)。"""
    params: list[dict] = [
        {"name": "region", "parameterType": {"type": "STRING"},
         "parameterValue": {"value": _region()}},
    ]
    name_col = _col_expr(cols["advertiser_name"])
    id_col = _col_expr(cols["advertiser_id"])
    creative_col = _col_expr(cols["creative_id"])
    whens, conds = [], []
    for i, (brand, mode, val, *rest) in enumerate(brand_specs):
        if mode in ("ids", "creatives"):
            # ids = 广告主账户白名单；creatives = 域名检索得到的素材 id（021）
            col, pname = (id_col, f"ids_{i}") if mode == "ids" else (creative_col, f"cr_{i}")
            cond = f"{col} IN UNNEST(@{pname})"
            params.append({"name": pname,
                           "parameterType": {"type": "ARRAY", "arrayType": {"type": "STRING"}},
                           "parameterValue": {"arrayValues": [{"value": v} for v in sorted(val)]}})
        else:
            cond = f"REGEXP_CONTAINS({name_col}, @re_{i})"
            params.append({"name": f"re_{i}", "parameterType": {"type": "STRING"},
                           "parameterValue": {"value": "(?i)" + str(val)}})
            excluded = sorted(rest[0]) if rest and rest[0] else []
            if excluded:
                cond = f"({cond} AND {id_col} NOT IN UNNEST(@ex_{i}))"
                params.append({"name": f"ex_{i}",
                               "parameterType": {"type": "ARRAY", "arrayType": {"type": "STRING"}},
                               "parameterValue": {"arrayValues": [{"value": v} for v in excluded]}})
        params.append({"name": f"brand_{i}", "parameterType": {"type": "STRING"},
                       "parameterValue": {"value": brand}})
        whens.append(f"WHEN {cond} THEN @brand_{i}")
        conds.append(cond)

    def opt(key: str, cast: str | None = None) -> str:
        p = cols.get(key)
        if not p:
            return "NULL"
        e = _col_expr(p)
        return f"SAFE_CAST({e} AS {cast})" if cast else e

    first = opt("first_shown", "DATE")
    last = opt("last_shown", "DATE")
    audience = f"TO_JSON_STRING({_col_expr(cols['audience'])})" if cols.get("audience") else "NULL"
    where = [f"{_col_expr(cols['region_code'])} = @region", "(" + " OR ".join(conds) + ")"]
    if cols.get("last_shown"):
        where.append(f"{last} >= @since")
        params.append({"name": "since", "parameterType": {"type": "DATE"},
                       "parameterValue": {"value": (date.today() - timedelta(
                           days=_lookback_days())).isoformat()}})
    sql = f"""
SELECT
  SAFE_CAST({id_col} AS STRING)                         AS advertiser_id,
  SAFE_CAST({_col_expr(cols['creative_id'])} AS STRING) AS creative_id,
  SAFE_CAST({name_col} AS STRING)                       AS advertiser_name,
  CASE {' '.join(whens)} END                            AS brand,
  SAFE_CAST({opt('ad_format')} AS STRING)               AS ad_format,
  SAFE_CAST({opt('page_url')} AS STRING)                AS page_url,
  CAST({first} AS STRING)                               AS first_shown,
  CAST({last} AS STRING)                                AS last_shown,
  {opt('shown_lower', 'INT64')}                         AS shown_lower,
  {opt('shown_upper', 'INT64')}                         AS shown_upper,
  {_surface_expr(cols.get('surface_code'), schema)}     AS surfaces,
  {audience}                                            AS audience
FROM `{BQ_TABLE}` AS c, UNNEST(c.region_stats) AS r
WHERE {' AND '.join(where)}
""".strip()
    return sql, params


def parse_bq_rows(resp: dict) -> list[dict]:
    """jobs.query / getQueryResults 的 f/v 行格式 → dict 列表（只支持标量与标量数组）。"""
    names = [f["name"] for f in ((resp.get("schema") or {}).get("fields") or [])]
    out = []
    for row in resp.get("rows") or []:
        rec = {}
        for name, cell in zip(names, row.get("f") or []):
            v = cell.get("v") if isinstance(cell, dict) else cell
            if isinstance(v, list):
                v = [x.get("v") if isinstance(x, dict) else x for x in v]
            rec[name] = v
        out.append(rec)
    return out


def google_row(rec: dict, run_id: str, region: str, today: date | None = None) -> dict:
    today = today or date.today()
    active_days = int(_env("CI_GOOGLE_ADS_ACTIVE_DAYS") or "7")
    first = _to_date(rec.get("first_shown"))
    last = _to_date(rec.get("last_shown"))
    lo = int(rec["shown_lower"]) if rec.get("shown_lower") not in (None, "") else None
    hi = int(rec["shown_upper"]) if rec.get("shown_upper") not in (None, "") else None
    aud = rec.get("audience")
    try:
        targeting = json.loads(aud) if aud else {}
    except ValueError:
        targeting = {"raw": aud}
    adv, cid = rec["advertiser_id"], rec["creative_id"]
    return {
        "source_code": GOOGLE_SOURCE,
        "ad_id": cid,
        "advertiser_id": adv,
        "advertiser_name": rec.get("advertiser_name"),
        "brand": rec.get("brand"),
        "ad_format": rec.get("ad_format"),
        "surfaces": sorted({s for s in (rec.get("surfaces") or []) if s}),
        "title": None,
        "body": None,                              # 数据集不含文案
        "languages": [],
        "first_shown": first,
        "last_shown": last,
        # 数据集只有「最后展示日」，没有投放状态：近 N 天内还在展示即视为在投
        "is_active": bool(last and last >= today - timedelta(days=active_days)),
        "exposure_lower": lo,
        "exposure_upper": hi,
        "exposure_kind": "impressions" if (lo is not None or hi is not None) else None,
        "targeting": targeting if isinstance(targeting, dict) else {"value": targeting},
        "product_ids": [],
        "ad_url": rec.get("page_url") or (
            f"https://adstransparency.google.com/advertiser/{adv}/creative/{cid}?region={region}"),
        "ingestion_run_id": run_id,
    }


class BigQuery:
    """最小 BigQuery REST 客户端：tables.get + jobs.query(+dryRun) + getQueryResults。

    不引 google-cloud-bigquery（重，且大半功能用不上）；只用 google-auth 换 token。
    """

    def __init__(self):
        raw = _env("GOOGLE_ADS_BQ_SA_JSON_B64")
        if not raw:
            raise RuntimeError("GOOGLE_ADS_BQ_SA_JSON_B64 未配置")
        from google.auth.transport.requests import Request
        from google.oauth2 import service_account
        info = json.loads(base64.b64decode(raw))
        creds = service_account.Credentials.from_service_account_info(info, scopes=[BQ_SCOPE])
        creds.refresh(Request())
        self.token = creds.token
        # 查询费记在这个项目上（公共数据集本身不收费，扫描量算你的）
        self.project = _env("GOOGLE_ADS_BQ_PROJECT") or info.get("project_id")
        self.last_estimate = 0
        if not self.project:
            raise RuntimeError("GOOGLE_ADS_BQ_PROJECT 未配置且 service account 里没有 project_id")

    def _call(self, method: str, path: str, *, body=None, params=None) -> dict:
        import httpx
        r = httpx.request(method, f"{BQ_BASE}/{path}", json=body, params=params,
                          headers={"Authorization": f"Bearer {self.token}"}, timeout=120)
        j = r.json() if r.content else {}
        if r.status_code >= 400:
            msg = (j.get("error") or {}).get("message") or r.text[:300]
            raise RuntimeError(f"BigQuery {method} {path.split('?')[0]} HTTP {r.status_code}: {msg}")
        return j

    def table_schema(self) -> dict[str, dict]:
        j = self._call("GET", BQ_TABLE_API, params={"fields": "schema"})
        return flatten_schema((j.get("schema") or {}).get("fields") or [])

    def estimate_bytes(self, sql: str, params: list[dict]) -> int:
        j = self._call("POST", f"projects/{self.project}/queries", body={
            "query": sql, "useLegacySql": False, "parameterMode": "NAMED",
            "queryParameters": params, "dryRun": True})
        return int(j.get("totalBytesProcessed") or 0)

    def query(self, sql: str, params: list[dict], max_rows: int = 200_000) -> list[dict]:
        j = self._call("POST", f"projects/{self.project}/queries", body={
            "query": sql, "useLegacySql": False, "parameterMode": "NAMED",
            "queryParameters": params, "timeoutMs": 90_000, "maxResults": 5000})
        ref = j.get("jobReference") or {}
        loc = ref.get("location")
        rows: list[dict] = []
        schema = None
        while True:
            # 列名只在带 schema 的响应里给；个别分页响应可能省略，沿用上一页的
            schema = j.get("schema") or schema
            if j.get("jobComplete"):
                rows += parse_bq_rows({**j, "schema": schema})
                if not j.get("pageToken") or len(rows) >= max_rows:
                    return rows
                page = {"pageToken": j["pageToken"]}
            else:
                page = {}                          # 还没跑完：不带 pageToken 轮询状态
            j = self._call("GET", f"projects/{ref['projectId']}/queries/{ref['jobId']}",
                           params={"location": loc, "maxResults": 5000, "timeoutMs": 60_000,
                                   **page})

    def guarded_query(self, sql: str, params: list[dict], logger) -> list[dict]:
        max_gb = float(_env("CI_GOOGLE_ADS_MAX_GB") or "200")
        est = self.estimate_bytes(sql, params)
        self.last_estimate = est
        logger.info("Google: 预估扫描 %.1f GB（上限 %.0f GB）", est / 1e9, max_gb)
        if est > max_gb * 1e9:
            raise CostGuard(f"预估扫描 {est / 1e9:.1f} GB 超过 CI_GOOGLE_ADS_MAX_GB={max_gb:.0f}")
        return self.query(sql, params)


def google_brand_specs(brands: dict[str, re.Pattern], allow: dict, deny: dict,
                       domain_cr: dict[str, set[str]] | None = None) -> list[tuple]:
    """domain_cr：登记了域名的品牌 → 本轮素材 id 集合（含检索失败时的兜底清单）。
    登记了域名的品牌与白名单同理，不再按品牌名检索；账户白名单仍然并用。"""
    domain_cr = domain_cr or {}
    specs = []
    for brand, brand_re in brands.items():
        ids = allow.get((GOOGLE_SOURCE, brand))
        blocked = deny.get((GOOGLE_SOURCE, brand), set())
        if ids:
            specs.append((brand, "ids", ids))
        if brand in domain_cr:
            if domain_cr[brand]:
                specs.append((brand, "creatives", domain_cr[brand]))
        elif not ids and OPT_OUT not in blocked:
            specs.append((brand, "regex", brand_re.pattern, blocked - {OPT_OUT}))
    return specs


# ---------------------------------------------------------------------------
# 按落地页域名归属（021）：透明度中心域名检索 → 素材 id
# ---------------------------------------------------------------------------
# 透明度中心网页（adstransparency.google.com）自己用的检索接口，**非官方、无文档**。
# 只拿「哪些素材点进去是这个域名」这一个事实，数字仍全部取自官方 BigQuery 数据集。
# 请求量极小（每域名每轮 1–3 页），逐页间隔 TC_PAGE_PAUSE 秒。Google 改版会让它失效：
# 解析不了就抛 DomainLookupFailed，上游退回已知素材清单并告警，绝不因此删数据。
TC_SEARCH_URL = "https://adstransparency.google.com/anji/_/rpc/SearchService/SearchCreatives"
TC_PAGE_SIZE = 40
TC_MAX_PAGES = 50           # 2000 条素材封顶；真撞到说明域名登记错了（登记成了大平台域名）
TC_PAGE_PAUSE = 1.0
# 透明度中心的地区码 = 2000 + ISO 3166-1 数字码。数据集只覆盖 EEA，只列 EEA。
TC_REGION_ISO_NUM = {
    "AT": 40, "BE": 56, "BG": 100, "HR": 191, "CY": 196, "CZ": 203, "DK": 208, "EE": 233,
    "FI": 246, "FR": 250, "DE": 276, "GR": 300, "HU": 348, "IS": 352, "IE": 372, "IT": 380,
    "LV": 428, "LI": 438, "LT": 440, "LU": 442, "MT": 470, "NL": 528, "NO": 578, "PL": 616,
    "PT": 620, "RO": 642, "SK": 703, "SI": 705, "ES": 724, "SE": 752,
}


class DomainLookupFailed(RuntimeError):
    """透明度中心域名检索拿不到可信结果（HTTP 错、结构变了、条数对不上）。"""


def domain_config() -> dict[str, list[str]]:
    """core.ci_ad_domain → 品牌 → [域名]（只取 active）。"""
    out: dict[str, list[str]] = {}
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT brand, domain FROM core.ci_ad_domain WHERE active ORDER BY 1, 2")
            for brand, domain in cur.fetchall():
                out.setdefault(brand, []).append(domain)
    return out


def tc_request(domain: str, region: str, page_token: str | None = None) -> dict:
    """SearchCreatives 的 f.req 负载：按域名、按地区，一页 TC_PAGE_SIZE 条。"""
    if region not in TC_REGION_ISO_NUM:
        raise DomainLookupFailed(f"地区 {region} 不在 EEA 列表里，透明度中心检索不支持")
    code = 2000 + TC_REGION_ISO_NUM[region]
    req = {"2": TC_PAGE_SIZE, "3": {"8": [code], "12": {"1": domain, "2": True}},
           "7": {"1": 1, "2": 0, "3": code}}
    if page_token:
        req["4"] = page_token
    return req


def parse_tc_page(j) -> tuple[dict[str, str], str | None, int | None]:
    """一页响应 → ({creative_id: advertiser_id}, 下一页 token, 报告的总条数)。

    字段是 protobuf 数字键：顶层 1=素材列表、2=下一页 token、4=总条数；
    素材里 1=广告主 id(AR…)、2=素材 id(CR…)。形状不对就抛，不猜。
    """
    if not isinstance(j, dict):
        raise DomainLookupFailed(f"响应不是 JSON 对象: {str(j)[:200]}")
    items = j.get("1") or []
    if not isinstance(items, list):
        raise DomainLookupFailed("响应字段 1 不是列表 —— 接口结构变了")
    out: dict[str, str] = {}
    for it in items:
        adv = it.get("1") if isinstance(it, dict) else None
        cid = it.get("2") if isinstance(it, dict) else None
        if not (isinstance(adv, str) and adv.startswith("AR")
                and isinstance(cid, str) and cid.startswith("CR")):
            raise DomainLookupFailed(f"素材条目结构变了: {str(it)[:200]}")
        out[cid] = adv
    total = j.get("4")
    try:
        total = int(total) if total not in (None, "") else None
    except (TypeError, ValueError):
        total = None
    token = j.get("2") if isinstance(j.get("2"), str) and j.get("2") else None
    return out, token, total


def tc_domain_creatives(domain: str, region: str, *, post=None) -> dict[str, str]:
    """按域名翻完所有页 → {creative_id: advertiser_id}。post 可注入，便于测试。"""
    import time
    if post is None:
        import httpx

        def post(req: dict) -> dict:
            r = httpx.post(TC_SEARCH_URL, params={"authuser": ""},
                           data={"f.req": json.dumps(req, separators=(",", ":"))},
                           headers={"User-Agent": "Mozilla/5.0 (compatible; ChannelHub-CI)"},
                           timeout=30)
            if r.status_code >= 400:
                raise DomainLookupFailed(f"HTTP {r.status_code}: {r.text[:200]}")
            try:
                return r.json()
            except ValueError:
                raise DomainLookupFailed(f"响应不是 JSON: {r.text[:200]}") from None
    found: dict[str, str] = {}
    token, total = None, None
    for page in range(TC_MAX_PAGES):
        if page:
            time.sleep(TC_PAGE_PAUSE)
        items, token, t = parse_tc_page(post(tc_request(domain, region, token)))
        total = t if t is not None else total
        found.update(items)
        if not token:
            break
    else:
        raise DomainLookupFailed(f"{domain} 翻了 {TC_MAX_PAGES} 页还没完 —— 域名是否登记成了大平台？")
    # 改版最可能的样子是「照样 200，但字段全没了」：既没素材也没总数，不能当成 0 条广告
    if not found and total is None:
        raise DomainLookupFailed(f"{domain} 响应里既没有素材也没有总数 —— 接口结构可能变了")
    # 报告的总数对不上 → 结果不完整，按失败处理（宁可沿用旧清单，也不拿半份清单去删数据）
    if total is not None and total != len(found):
        raise DomainLookupFailed(f"{domain} 报告 {total} 条素材，实际取到 {len(found)} 条")
    return found


def known_creatives(source_code: str, brand: str) -> set[str]:
    """raw.ci_ad 里该品牌已有的素材 id —— 域名检索失败时的兜底清单。"""
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ad_id FROM raw.ci_ad WHERE source_code = %s AND brand = %s",
                        (source_code, brand))
            return {r[0] for r in cur.fetchall()}


def resolve_domain_creatives(domains: dict[str, list[str]], region: str, logger, *,
                             lookup=tc_domain_creatives, known=known_creatives
                             ) -> tuple[dict[str, set[str]], dict[str, set[str]], list[str]]:
    """品牌 → 素材 id。返回 (用于查询的清单, 可据此清理旧行的清单, 失败说明)。

    某品牌任一域名检索失败 → 该品牌退回「已知素材 ∪ 本轮成功域名的素材」继续刷新，
    但不进第二个返回值（不据此删行）。
    全部域名都「成功」却一条素材都没有、而库里原本有 → 同样按失败处理：透明度中心保留
    历史广告，域名的素材从几十条掉到 0 几乎只可能是接口出了问题，不能据此清空该品牌。
    """
    use: dict[str, set[str]] = {}
    prunable: dict[str, set[str]] = {}
    failures: list[str] = []
    for brand, doms in domains.items():
        cids: set[str] = set()
        ok = True
        for d in doms:
            try:
                got = lookup(d, region)
                logger.info("Google: 域名 %s → %d 条素材（%s）", d, len(got),
                            ", ".join(sorted(set(got.values()))) or "无")
                cids |= set(got)
            except DomainLookupFailed as e:
                ok = False
                failures.append(f"{brand} / {d}: {e}")
                logger.warning("Google: 域名 %s 检索失败: %s", d, e)
        fallback = None
        if ok and not cids:
            fallback = known(GOOGLE_SOURCE, brand)
            if fallback:
                ok = False
                failures.append(f"{brand} / {', '.join(doms)}: 域名检索返回 0 条素材，"
                                f"但库里已有 {len(fallback)} 条 —— 疑似接口改版")
                logger.warning("Google: %s 域名检索返回 0 条但库里已有 %d 条", brand, len(fallback))
        if ok:
            prunable[brand] = cids
        else:
            fallback = fallback if fallback is not None else known(GOOGLE_SOURCE, brand)
            logger.warning("Google: %s 退回已知素材清单（%d 条）", brand, len(fallback))
            cids |= fallback
        use[brand] = cids
    return use, prunable, failures


@task(retries=1, retry_delay_seconds=120)
def collect_google_ads(run_id: str) -> dict:
    logger = _log()
    stats = {"ads": 0, "schema_changed": 0, "cost_guard": 0, "gb_estimated": 0.0}
    region = _region()
    if not _env("GOOGLE_ADS_BQ_SA_JSON_B64"):
        logger.info("Google: 未配置 GOOGLE_ADS_BQ_SA_JSON_B64，跳过")
        return stats
    bq = BigQuery()
    schema = bq.table_schema()
    try:
        cols = resolve_columns(schema)
    except SchemaChanged as e:
        logger.warning("Google: %s", e)
        stats["schema_changed"] = 1
        stats["detail"] = str(e)
        return stats
    missing_opt = [k for k in OPTIONAL_COLS if not cols.get(k)]
    if missing_opt:
        logger.warning("Google: 可选列缺失（对应字段将为空）: %s", ", ".join(missing_opt))
    products = load_products()
    brands = brand_targets(products)
    allow, deny = advertiser_config()
    domains = {b: d for b, d in domain_config().items() if b in brands}
    domain_cr, prunable, failures = resolve_domain_creatives(domains, region, logger)
    stats["domain_creatives"] = sum(len(v) for v in domain_cr.values())
    if failures:
        stats["domain_lookup_failed"] = 1
        stats["domain_detail"] = "\n".join(failures)
    specs = google_brand_specs(brands, allow, deny, domain_cr)
    if not specs:
        logger.info("Google: 所有品牌都禁用了按名检索且无白名单 id / 域名素材，跳过查询")
        stats["pruned"] = _prune(GOOGLE_SOURCE, brands, allow, deny, prunable)
        return stats
    sql, params = build_google_sql(cols, schema, specs)
    try:
        recs = bq.guarded_query(sql, params, logger)
    except CostGuard as e:
        logger.warning("Google: %s", e)
        stats["cost_guard"] = 1
        stats["detail"] = str(e)
        return stats
    finally:
        stats["gb_estimated"] = round(bq.last_estimate / 1e9, 1)
    rows = [google_row(r, run_id, region) for r in recs if r.get("brand") and r.get("creative_id")]
    stats["ads"] = _write_ads(rows)
    stats["pruned"] = _prune(GOOGLE_SOURCE, brands, allow, deny, prunable)
    return stats


# ===========================================================================
# 五、编排
# ===========================================================================
@flow(name="ci-ads")
def ci_ads() -> dict:
    from ci_price import _active_sources   # 单一实现；延迟导入免得 discover 命令拖进价格层依赖
    logger = get_run_logger()
    run_id = str(getattr(flow_run, "id", "") or "")
    total = {"sources": 0, "failed": 0, "ads": 0}
    if is_dry_run():
        logger.warning("CI_DRY_RUN=true —— 照常查询解析但不写库")
    active = _active_sources()
    for name, job in ((META_SOURCE, lambda: collect_meta_ads(run_id)),
                      (GOOGLE_SOURCE, lambda: collect_google_ads(run_id))):
        if name not in active:
            logger.info("%s 在 core.ci_source 中已停用，跳过", name)
            continue
        total["sources"] += 1
        try:
            st = job()
            total["ads"] += st.get("ads", 0)
            total[name] = st
            logger.info("%s 完成: %s", name, st)
            if st.get("auth_error"):
                maybe_alert(name, "api_auth_required",
                            "Ad Library API 拒绝了 token（过期或未完成身份确认）。"
                            "0 条结果不代表竞品没投广告。", logger)
            if st.get("schema_changed"):
                maybe_alert(name, "schema_changed", st.get("detail", ""), logger)
            if st.get("cost_guard"):
                maybe_alert(name, "cost_guard", st.get("detail", ""), logger)
            if st.get("domain_lookup_failed"):
                maybe_alert(name, "domain_lookup_failed",
                            "透明度中心按域名检索失败，本轮沿用已知素材清单（新上的广告没进来）。\n"
                            "多半是网页接口改版，见 flows/ci_ads.py parse_tc_page。\n\n"
                            + st.get("domain_detail", ""), logger)
        except Exception as e:
            total["failed"] += 1
            logger.warning("%s 失败: %s", name, e, exc_info=True)
            try:
                maybe_alert(name, "collect_failed", f"{type(e).__name__}: {e}", logger)
            except Exception:
                logger.warning("%s 告警本身也失败了", name)
    logger.info("ci-ads 汇总: %s", total)
    if total["failed"]:
        raise RuntimeError(f"{total['failed']} 个源采集失败: {total}")
    return total


# ===========================================================================
# 六、发现命令：列出候选广告主 id，粘进 db/seed/ci_ad_advertiser.csv
# ===========================================================================
def discover(brands_filter: list[str]) -> None:
    """用法（worker 容器内）:
         docker compose exec prefect-worker python flows/ci_ads.py discover [品牌 …]
       只读：不写库。Google 这一步只扫广告主两列，dryRun 预估后才真查。
    """
    import csv
    logger = _log()
    products = load_products()
    brands = {b: r for b, r in brand_targets(products).items()
              if not brands_filter or b.lower() in {x.lower() for x in brands_filter}}
    out = csv.writer(sys.stdout, lineterminator="\n")   # 广告主名里可能有引号/逗号
    out.writerow(["source_code", "advertiser_id", "brand", "advertiser_name", "active", "notes"])

    token = _env("META_AD_LIBRARY_TOKEN")
    if token:
        for brand, brand_re in brands.items():
            seen: dict[tuple[str, str], int] = {}
            params = {"access_token": token, "ad_reached_countries": json.dumps([_region()]),
                      "ad_type": "ALL", "ad_active_status": "ALL", "search_terms": brand,
                      "fields": "page_id,page_name", "limit": 200}
            for kind, data in _meta_pages(params, logger, 3):
                if kind == "error":
                    logger.warning("Meta [%s] %s: %s", brand, meta_error_kind(data),
                                   str(data.get("message"))[:200])
                    break
                for ad in data:
                    k = (str(ad.get("page_id")), ad.get("page_name") or "")
                    seen[k] = seen.get(k, 0) + 1
            for (pid, pname), n in sorted(seen.items(), key=lambda kv: -kv[1]):
                # 主页名过不了品牌正则的多半是转售商：照样列出，但 active 标 false 供人工判断
                ok = "true" if brand_re.search(pname) else "false"
                out.writerow(["meta_ads", pid, brand, pname, ok, f"discover: {n} 条广告"])
    else:
        logger.info("Meta: 未配置 META_AD_LIBRARY_TOKEN，跳过发现")

    if _env("GOOGLE_ADS_BQ_SA_JSON_B64"):
        bq = BigQuery()
        cols = resolve_columns(bq.table_schema())
        name_col, id_col = cols["advertiser_name"], cols["advertiser_id"]
        for brand, brand_re in brands.items():
            sql = (f"SELECT {id_col} AS advertiser_id, {name_col} AS advertiser_name, "
                   f"COUNT(*) AS creatives FROM `{BQ_TABLE}` "
                   f"WHERE REGEXP_CONTAINS({name_col}, @re) GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 50")
            params = [{"name": "re", "parameterType": {"type": "STRING"},
                       "parameterValue": {"value": "(?i)" + brand_re.pattern}}]
            for rec in bq.guarded_query(sql, params, logger):
                out.writerow(["google_ads", rec["advertiser_id"], brand, rec["advertiser_name"],
                              "true", f"discover: {rec['creatives']} 条素材(全 EEA)"])
    else:
        logger.info("Google: 未配置 GOOGLE_ADS_BQ_SA_JSON_B64，跳过发现")
    print("# 核对 advertiser_name 确是该品牌官方后，把行粘进 db/seed/ci_ad_advertiser.csv，"
          "再跑 bash db/seed/load_ci_ad_advertiser.sh", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "discover":
        discover(sys.argv[2:])
    else:
        ci_ads()

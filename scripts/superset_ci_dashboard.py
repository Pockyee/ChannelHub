"""Superset 一键搭建竞品情报看板(幂等)。每条情报线(core.ci_product.line)一个看板:
  · HUTT Competitive Intelligence  (line=hutt, slug=competitive-intel)       HUTT vs ECOVACS 擦窗机
  · imoo Competitive Intelligence  (line=imoo, slug=imoo-competitive-intel)  imoo vs Xplora 儿童手表
两个看板共用同一批数据集与图表结构,每张图都带 line == <线> 的过滤;新增一条线只需在
DASHBOARDS 里加一项(图名前缀必须不同 —— 本脚本按图名认领已有图表)。

前置:
  · db/migrations/012→013→014 已应用(存在 mart.v_ci_* 视图)
  · scripts/superset_setup.py 已跑过(存在数据源 "ChannelHub")

做四件事(全部幂等:存在则更新,不存在则创建):
  1) 注册 7 个数据集:v_ci_compare(主) / v_ci_share_of_voice / v_ci_mention_detail /
     v_ci_ad_detail(广告层,019) / v_ci_ad_daily(广告展示按日摊分,020) /
     v_ci_search_trend + v_ci_app_daily(需求层,022)
  2) 每个看板建 5 张图(图名带线前缀,如「HUTT CI · Mentions」),每张独占一行:
       A Price Trend by Model      折线时序,按产品分组;只画有报价的日子,
                                   横轴按实际数据起止拉伸(视图里的纯提及日
                                   best_total_eur 为 NULL,不过滤会把横轴撑到 2025)
       B Mentions per Week         柱状     —— 提及量
       C Mentions                  表格     —— 全部提及,按 mention_kind 切档
                                              (social_media/test/promo/media_review/discussion)
       D Ad Impressions (est.)     折线     —— 每个时间段的广告展示次数(估算),按品牌分色;
                                              周期由 Impressions period 过滤器切 日/周/月
       E Ads                       表格     —— Meta + Google 广告明细,可点开原广告
     DASHBOARDS 里带 "demand": True 的看板(目前只有 imoo)再加需求层 5 张图,不挂时间过滤器:
       F Search Interest (Google Trends)  折线 —— 品牌词/品类词 5 年周序列
       G Amazon Bestseller Rank           折线 —— 倒置 y 轴,越低越好
       H Amazon Ratings                   折线 —— 累计评分数(增速 ≈ 销量代理)
       I App Ratings                      折线 —— 配套 App 评分数(≈ 装机量)
       J App Installs (Google Play)       折线 —— 精确安装数(全球口径)
     (图表名与指标标签一律英文:看板是给人看的对外产物,注释保持中文)
  3) 组装成看板,带五个原生过滤器:
       · Price window  —— 只作用于 A 图的时间区间,相对今天倒推,默认最近 30 天(含今天)
       · Mentions & Ads window —— B/C/D/E 四张图共用的时间区间,默认最近一年(含今天)
       · Mention kind  —— 提及表切档
       · Ad platform   —— 只作用于 D/E 两张广告图,Meta / Google 切换
       · Impressions period —— 只作用于 D 图的时间粒度,默认按周
  4) 把图挂到看板;并清理被改名/移除的旧图(声明式)

口径说明见 db/migrations/014_ci_mart.sql 与 docs/COMPETITIVE_INTEL.md:
  · best_total_eur = 跨源最低到手价(售价+运费)

价格类图在 db/seed/ci_product_alias.csv 填好各源商品标识、ci-price 跑过之前是空的,
这是预期行为而非故障(见 docs/COMPETITIVE_INTEL.md「唯一的人工前置」)。

环境变量(与 superset_setup.py 一致):
  SUPERSET_URL(默认 http://superset:8088)
  SUPERSET_ADMIN_USERNAME  SUPERSET_ADMIN_PASSWORD

在 docker 网络内运行:
  docker run --rm --network channelhub_channelhub --env-file .env \\
    -v "$PWD/scripts/superset_ci_dashboard.py:/dash.py:ro" \\
    prefecthq/prefect:3-latest python /dash.py
"""
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.request

_opener = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN_USER = os.environ["SUPERSET_ADMIN_USERNAME"]
ADMIN_PW = os.environ["SUPERSET_ADMIN_PASSWORD"]
DB_NAME = "ChannelHub"
SCHEMA = "mart"

# 一条情报线一个看板。HUTT 沿用改名前的 slug 与过滤器 id,老链接与已存的过滤状态不失效。
DASHBOARDS = [
    {"line": "hutt", "slug": "competitive-intel", "title": "HUTT Competitive Intelligence",
     "chart_prefix": "HUTT CI", "filter_prefix": "NATIVE_FILTER-ci"},
    {"line": "imoo", "slug": "imoo-competitive-intel", "title": "imoo Competitive Intelligence",
     "chart_prefix": "imoo CI", "filter_prefix": "NATIVE_FILTER-imoo-ci", "demand": True},
]

DATASETS = {                      # 表名 → 主时间列
    "v_ci_compare": "observed_on",
    "v_ci_share_of_voice": "mention_week",
    "v_ci_mention_detail": "published_at",
    "v_ci_ad_detail": "first_shown",          # 广告层(019)
    "v_ci_ad_daily": "ad_day",                # 广告展示按日摊分(020)
    "v_ci_search_trend": "trend_week",        # 需求层:Google Trends(022)
    "v_ci_app_daily": "observed_on",          # 需求层:App 商店指标(022)
}


def call(method, path, *, token=None, csrf=None, body=None):
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if csrf:
        headers["X-CSRFToken"] = csrf
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=headers)
    try:
        with _opener.open(req) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def die(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def list_all(kind, token):
    out, page = [], 0
    while True:
        st, j = call("GET", f"/api/v1/{kind}/?q=(page:{page},page_size:100)", token=token)
        if st != 200:
            die(f"列出 {kind} 失败: {st} {j}")
        rows = j.get("result") or []
        out += rows
        if len(rows) < 100:
            return out
        page += 1


# ---------------------------------------------------------------------------
# 指标 / 过滤 构造(adhoc,免在数据集预建指标)
# ---------------------------------------------------------------------------
def m(col, label, agg="SUM", opt=None):
    return {"expressionType": "SIMPLE", "column": {"column_name": col},
            "aggregate": agg, "label": label,
            "optionName": opt or f"metric_{col}_{agg.lower()}"}


BEST_PRICE = m("best_total_eur", "Best Price (€)", agg="MIN", opt="metric_best_price")
MENTION_CNT = m("mention_cnt", "Mentions", agg="SUM", opt="metric_mention_cnt")
AD_IMPR = m("impressions_est", "Impressions (est.)", agg="SUM", opt="metric_ad_impr")
SEARCH_IDX = m("value", "Search interest (0–100)", agg="MAX", opt="metric_search_idx")
AMZ_BSR = m("amazon_bsr", "Amazon BSR (lower = better)", agg="MIN", opt="metric_amz_bsr")
AMZ_RATINGS = m("amazon_review_count", "Amazon ratings", agg="MAX", opt="metric_amz_ratings")
APP_RATINGS = m("rating_count", "App ratings", agg="MAX", opt="metric_app_ratings")
APP_INSTALLS = m("installs_exact", "Installs (worldwide)", agg="MAX", opt="metric_app_installs")


def flt(subject, op, comparator, name):
    op_id = {"==": "EQUALS", "IS NOT NULL": "IS_NOT_NULL"}[op]
    return {"expressionType": "SIMPLE", "subject": subject, "operator": op,
            "operatorId": op_id, "comparator": comparator, "clause": "WHERE",
            "filterOptionName": name}


HAS_PRICE = flt("best_total_eur", "IS NOT NULL", None, "filter_has_price")
HAS_BSR = flt("amazon_bsr", "IS NOT NULL", None, "filter_has_bsr")
HAS_AMZ_RATINGS = flt("amazon_review_count", "IS NOT NULL", None, "filter_has_amz_ratings")
ON_PLAY = flt("store", "==", "google_play", "filter_store_play")

# 需求层(022)图表的说明文字,挂在图表的 description 上(看板里悬停标题可见)
CHART_NOTES = {
    "Search Interest (Google Trends)":
        "Google Trends, Germany, weekly, last 5 years. 100 = the highest week of any term in "
        "this one request; values are only comparable within this chart.",
    "Amazon Bestseller Rank":
        "Amazon.de bestseller rank in Elektronik & Foto (lower = better). Rank is per parent "
        "listing: imoo Z1/Z3/Z7/X10 share one parent, so they show the same rank; imoo has a "
        "second parent listing ('Amazon listing 2') with its own rank. 'Xplora X6Play (Telekom "
        "eSIM)' is the carrier version on Amazon.",
    "Amazon Ratings":
        "Cumulative star ratings on the tracked amazon.de listing. Growth over time is a "
        "sales proxy.",
    "App Ratings":
        "Cumulative ratings of the companion app. App Store = Germany only; Google Play = "
        "worldwide. The watch needs the app, so this approximates the installed base.",
    "App Installs (Google Play)":
        "Exact install count embedded in the Play Store page, worldwide (not Germany only). "
        "imoo is large in Asia, so compare trends rather than levels.",
}


def query_context(ds_id, form_data, *, columns, metrics, is_timeseries=False,
                  x_axis=None, row_limit=None, orderby=None, granularity=None):
    q = {
        "filters": [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
                    for f in form_data.get("adhoc_filters", [])],
        "columns": columns,
        "metrics": metrics,
        "orderby": orderby or ([[metrics[0], False]] if metrics else []),
        "annotation_layers": [],
        "row_limit": row_limit or form_data.get("row_limit", 10000),
        "series_limit": 0,
        "order_desc": True,
        "url_params": {},
        "custom_params": {},
        "custom_form_data": {},
    }
    if is_timeseries:
        q["is_timeseries"] = True
    if granularity:
        q["granularity"] = granularity
    if x_axis:
        q["x_axis"] = x_axis
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False,
            "queries": [q], "form_data": form_data,
            "result_format": "json", "result_type": "full"}


# ---------------------------------------------------------------------------
# 图定义:返回 [(名称, viz_type, form_data, query_context), ...]
# ---------------------------------------------------------------------------
def chart_defs(ids, line, prefix, demand=False):
    line_f = flt("line", "==", line, "filter_line")
    cmp_id = ids["v_ci_compare"]
    sov_id = ids["v_ci_share_of_voice"]
    det_id = ids["v_ci_mention_detail"]
    ad_id = ids["v_ci_ad_detail"]
    ad_day_id = ids["v_ci_ad_daily"]
    out = []

    def ts(name, ds_id, x, metric, filters, *, invert=False, series="line", fmt=",.2f",
           groupby="display_name", grain=None):
        fd = {"datasource": f"{ds_id}__table", "url_params": {},
              "viz_type": "echarts_timeseries_" + series,
              "x_axis": x, "granularity_sqla": x, "time_grain_sqla": grain,
              "metrics": [metric], "groupby": [groupby],
              "adhoc_filters": filters, "row_limit": 10000,
              "x_axis_sort_asc": True, "show_legend": True,
              "markerEnabled": True, "rich_tooltip": True,
              "y_axis_format": fmt, "y_axis_reverse": invert,
              "seriesType": series}
        qc = query_context(ds_id, fd, columns=[x, groupby], metrics=[metric],
                           is_timeseries=True, x_axis=x, granularity=x,
                           orderby=[[x, True]])
        out.append((f"{prefix} · {name}", "echarts_timeseries_" + series, fd, qc))

    # v_ci_compare 是「报价日 ∪ 提及日」的并集:只有提及没有报价的日子 best_total_eur
    # 为 NULL。不过滤的话横轴会被 2025 年的提及行撑开,曲线前面一大段全是空的。
    # 只画有价格的日子,横轴就按实际报价数据的起止拉伸。
    ts("Price Trend by Model", cmp_id, "observed_on", BEST_PRICE, [line_f, HAS_PRICE])
    ts("Mentions per Week", sov_id, "mention_week", MENTION_CNT, [line_f],
       series="bar", fmt="SMART_NUMBER")

    # 全量提及明细表（不再只看媒体层）
    # mention_kind 让看板一张表覆盖 test / promo / media_review / discussion，
    # 靠看板上的 native filter 切档，不必为每一档单独建图。
    # title_link 是视图里拼好的 <a>；allow_render_html 打开后才会被渲染成链接，
    # 否则 Superset 把单元格当纯文本，屏幕上就是一串 <a href=…> 源码。
    # time_col = 看板时间窗口过滤的列(granularity_sqla);不设的话 Superset 不知道按哪列截。
    def table(name, ds_id, cols, order_col, time_col):
        fd = {"datasource": f"{ds_id}__table", "url_params": {},
              "viz_type": "table", "query_mode": "raw",
              "all_columns": cols, "granularity_sqla": time_col,
              "allow_render_html": True,
              "adhoc_filters": [line_f], "row_limit": 500,
              "order_by_cols": [f'["{order_col}", false]'],
              "table_timestamp_format": "smart_date"}
        qc = query_context(ds_id, fd, columns=cols, metrics=[], row_limit=500,
                           orderby=[[order_col, False]], granularity=time_col)
        qc["queries"][0]["result_type"] = "results"
        out.append((f"{prefix} · {name}", "table", fd, qc))

    table("Mentions", det_id,
          ["published_on", "mention_kind", "display_name", "outlet", "title_link"],
          "published_on", "published_on")

    # 广告层(Meta Ad Library + Google Ads Transparency),呈现与 Mentions 对齐:
    # 上面一张展示次数走势,下面一张可点击明细表。
    # 走势图用 v_ci_ad_daily:源只给每条广告的生命周期累计区间,视图把区间中点平均摊到
    # 首末展示日,这里按时间粒度(默认周,看板上可切日/月)加总 —— 是估算,口径见 020。
    # 只含 Google impressions;Meta 给的是去重覆盖人数(reach),跨天加总无意义。
    ts("Ad Impressions (est.)", ad_day_id, "ad_day", AD_IMPR, [line_f],
       fmt="SMART_NUMBER", groupby="brand", grain="P1W")
    table("Ads", ad_id,
          ["first_shown", "last_shown", "status", "platform", "brand", "advertiser_name",
           "surfaces", "exposure", "products", "ad_link"],
          # 时间窗口按 last_shown 截:窗口内「还在展示」的广告都列出,
          # 包括窗口开始前就开投的长期广告(按 first_shown 截会把它们漏掉)
          "first_shown", "last_shown")

    # 需求层(022):搜索热度 / Amazon 排名与评分数 / App 评分数与安装量。
    # 不挂任何时间过滤器(看全历史):Trends 一次就给 5 年,App/Amazon 从 2026-09 起逐日累积。
    if demand:
        ts("Search Interest (Google Trends)", ids["v_ci_search_trend"], "trend_week",
           SEARCH_IDX, [line_f], fmt="SMART_NUMBER", groupby="term")
        ts("Amazon Bestseller Rank", cmp_id, "observed_on", AMZ_BSR, [line_f, HAS_BSR],
           invert=True, fmt="SMART_NUMBER")
        ts("Amazon Ratings", cmp_id, "observed_on", AMZ_RATINGS, [line_f, HAS_AMZ_RATINGS],
           fmt="SMART_NUMBER")
        ts("App Ratings", ids["v_ci_app_daily"], "observed_on", APP_RATINGS, [line_f],
           fmt="SMART_NUMBER", groupby="app_label")
        ts("App Installs (Google Play)", ids["v_ci_app_daily"], "observed_on", APP_INSTALLS,
           [line_f, ON_PLAY], fmt="SMART_NUMBER", groupby="app_label")
    return out


# 看板布局:每行几张图 + 行高(下标对应 chart_defs 的输出顺序)
LAYOUT_ROWS = [
    {"charts": [0], "height": 60},           # 价格走势 —— 独占一行
    {"charts": [1], "height": 50},           # 声量柱状图 —— 独占一行
    {"charts": [2], "height": 70},           # 全量提及明细表 —— 独占一行
    {"charts": [3], "height": 50},           # 广告展示次数走势 —— 独占一行
    {"charts": [4], "height": 70},           # 广告明细表 —— 独占一行
]
DEMAND_ROWS = [                              # 需求层(只在 demand=True 的看板)
    {"charts": [5], "height": 55},           # 搜索热度 —— 独占一行
    {"charts": [6, 7], "height": 50},        # Amazon 排名 | Amazon 评分数
    {"charts": [8, 9], "height": 50},        # App 评分数 | App 安装量
]


def position_json(chart_ids, chart_names, title):
    rows = LAYOUT_ROWS + (DEMAND_ROWS if len(chart_ids) > 5 else [])
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
    }
    row_ids = []
    for row_no, row in enumerate(rows):
        row_id = f"ROW-{row_no}"
        row_ids.append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [],
                       "parents": ["ROOT_ID", "GRID_ID"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(row["charts"])
        for idx in row["charts"]:
            comp_id = f"CHART-{idx}"
            pos[row_id]["children"].append(comp_id)
            pos[comp_id] = {"type": "CHART", "id": comp_id, "children": [],
                            "parents": ["ROOT_ID", "GRID_ID", row_id],
                            "meta": {"chartId": chart_ids[idx], "uuid": None,
                                     "sliceName": chart_names[idx],
                                     "width": width, "height": row["height"]}}
    pos["GRID_ID"] = {"type": "GRID", "id": "GRID_ID",
                      "children": row_ids, "parents": ["ROOT_ID"]}
    return pos


# ===========================================================================
# 主流程
# ===========================================================================
st, j = call("POST", "/api/v1/security/login", body={
    "username": ADMIN_USER, "password": ADMIN_PW, "provider": "db", "refresh": False})
if st != 200 or "access_token" not in j:
    die(f"登录失败: {st} {j}")
token = j["access_token"]
st, j = call("GET", "/api/v1/security/csrf_token/", token=token)
if st != 200:
    die(f"获取 CSRF 失败: {st} {j}")
csrf = j["result"]
print("登录 OK")

dbs = list_all("database", token)
db = next((d for d in dbs if d.get("database_name") == DB_NAME), None)
if not db:
    die(f"未找到数据源 [{DB_NAME}],请先跑 scripts/superset_setup.py")
db_id = db["id"]
print(f"数据源 [{DB_NAME}] id={db_id}")

all_ds = list_all("dataset", token)
ds_ids = {}
for table, dttm in DATASETS.items():
    ds = next((d for d in all_ds
               if d.get("table_name") == table and d.get("schema") == SCHEMA), None)
    if ds:
        ds_id = ds["id"]
        action = "已存在"
    else:
        st, j = call("POST", "/api/v1/dataset/", token=token, csrf=csrf, body={
            "database": db_id, "schema": SCHEMA, "table_name": table})
        if st not in (200, 201):
            die(f"创建数据集 {SCHEMA}.{table} 失败: {st} {j}")
        ds_id = j["id"]
        action = "已创建"
    ds_ids[table] = ds_id
    # 从源同步列(视图改了列才认得),并设主时间列
    call("PUT", f"/api/v1/dataset/{ds_id}/refresh", token=token, csrf=csrf, body={})
    call("PUT", f"/api/v1/dataset/{ds_id}", token=token, csrf=csrf,
         body={"main_dttm_col": dttm})
    print(f"数据集 {SCHEMA}.{table} {action} id={ds_id}")


def _scope_to(flt, names, chart_ids, chart_names):
    """把过滤器收窄到指定图:其余图全部 excluded。过滤器列只存在于对应数据集,
    不收窄的话别的图会在看板上挂一个「不适用的过滤器」提示。"""
    keep = [chart_ids[chart_names.index(n)] for n in names]
    flt["scope"]["excluded"] = [c for c in chart_ids if c not in keep]
    flt["chartsInScope"] = keep
    flt["tabsInScope"] = []


# 价格图的时间区间过滤:相对「今天」倒推(Last week / Last month / Last quarter /
# 自定义),默认最近 30 天(含今天)。只作用于价格图 —— 声量与广告图另有一个共用窗口
# (Mentions & Ads window,默认一年),价格看近况、声量/广告看长期,不该被同一个窗口截断。
PRICE_WINDOW_DEFAULT = ('DATEADD(DATETIME("today"), -30, day) : '
                        'DATEADD(DATETIME("today"), 1, day)')
# 声量与广告四张图共用的时间窗口,默认最近一年;上界同样写到明天零点,今天的数据不漏。
ACTIVITY_WINDOW_DEFAULT = ('DATEADD(DATETIME("today"), -1, year) : '
                           'DATEADD(DATETIME("today"), 1, day)')


def build_dashboard(cfg, existing_charts, dashboards):
    line, slug, title = cfg["line"], cfg["slug"], cfg["title"]
    prefix, fid = cfg["chart_prefix"], cfg["filter_prefix"]
    print(f"--- 看板 [{title}] (line={line}) ---")

    chart_ids, chart_names = [], []
    for name, viz, fd, qc in chart_defs(ds_ids, line, prefix, demand=cfg.get("demand", False)):
        ds_id = int(fd["datasource"].split("__")[0])
        body = {"slice_name": name, "viz_type": viz,
                "datasource_id": ds_id, "datasource_type": "table",
                "params": json.dumps(fd, ensure_ascii=False),
                "query_context": json.dumps(qc, ensure_ascii=False),
                "description": CHART_NOTES.get(name.split(" · ", 1)[-1])}
        if name in existing_charts:
            cid = existing_charts[name]
            st, j = call("PUT", f"/api/v1/chart/{cid}", token=token, csrf=csrf, body=body)
            action = "更新"
        else:
            st, j = call("POST", "/api/v1/chart/", token=token, csrf=csrf, body=body)
            cid = j.get("id")
            action = "创建"
        if st not in (200, 201):
            die(f"{action}图表 [{name}] 失败: {st} {j}")
        chart_ids.append(cid)
        chart_names.append(name)
        print(f"图表 [{name}] {action} OK id={cid}")

    pos = position_json(chart_ids, chart_names, title)
    dash = next((d for d in dashboards if d.get("slug") == slug), None)
    old_chart_ids = set()
    if dash:
        _, dj = call("GET", f"/api/v1/dashboard/{dash['id']}", token=token)
        try:
            oldpos = json.loads((dj.get("result") or {}).get("position_json") or "{}")
            old_chart_ids = {v["meta"]["chartId"] for v in oldpos.values()
                             if isinstance(v, dict) and v.get("type") == "CHART"
                             and v.get("meta", {}).get("chartId")}
        except (ValueError, KeyError, TypeError):
            pass
    # 看板级 native filter：按提及类型切档。id 写死成固定串而不是随机 uuid ——
    # 本脚本每次 deploy 重放，随机 id 会每次生成一个新过滤器，看板上越堆越多。
    kind_filter = {
        "id": f"{fid}-mention-kind",
        "name": "Mention kind",
        "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "targets": [{"datasetId": ds_ids["v_ci_mention_detail"],
                     "column": {"name": "mention_kind"}}],
        "controlValues": {"multiSelect": True, "enableEmptyFilter": False,
                          "searchAllOptions": False, "inverseSelection": False},
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},     # 下方按图名收窄
        "cascadeParentIds": [],
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "description": "social_media / test / promo / media_review / discussion / other",
    }
    # 广告平台切换(Meta / Google),只作用于两张广告图。
    platform_filter = {
        "id": f"{fid}-ad-platform",
        "name": "Ad platform",
        "filterType": "filter_select",
        "type": "NATIVE_FILTER",
        "targets": [{"datasetId": ds_ids["v_ci_ad_detail"], "column": {"name": "platform"}}],
        "controlValues": {"multiSelect": True, "enableEmptyFilter": False,
                          "searchAllOptions": False, "inverseSelection": False},
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "cascadeParentIds": [],
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "description": "Meta (Facebook/Instagram) / Google (YouTube/Search/…)",
    }
    _scope_to(kind_filter, [f"{prefix} · Mentions"], chart_ids, chart_names)
    _scope_to(platform_filter, [f"{prefix} · Ad Impressions (est.)", f"{prefix} · Ads"],
              chart_ids, chart_names)
    # 展示次数走势的时间粒度(日/周/月…),默认按周;只作用于该图。
    grain_filter = {
        "id": f"{fid}-impr-period",
        "name": "Impressions period",
        "filterType": "filter_timegrain",
        "type": "NATIVE_FILTER",
        "targets": [{"datasetId": ds_ids["v_ci_ad_daily"]}],
        "controlValues": {},
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "cascadeParentIds": [],
        "defaultDataMask": {"extraFormData": {"time_grain_sqla": "P1W"},
                            "filterState": {"value": ["P1W"]}, "ownState": {}},
        "description": "Ad Impressions only — aggregate per day / week / month",
    }
    _scope_to(grain_filter, [f"{prefix} · Ad Impressions (est.)"], chart_ids, chart_names)
    # filter_time 没有数据集目标,targets 固定写 [{}];作用范围用 excluded 排除其它图。
    price_chart_id = chart_ids[chart_names.index(f"{prefix} · Price Trend by Model")]
    other_chart_ids = [c for c in chart_ids if c != price_chart_id]
    price_time_filter = {
        "id": f"{fid}-price-window",
        "name": "Price window",
        "filterType": "filter_time",
        "type": "NATIVE_FILTER",
        "targets": [{}],
        "controlValues": {},
        "scope": {"rootPath": ["ROOT_ID"], "excluded": other_chart_ids},
        "chartsInScope": [price_chart_id],
        "tabsInScope": [],
        "cascadeParentIds": [],
        # 默认窗口不用内置的 "Last month":它的上界是今天零点,会把当天 05:00 采到的
        # 点漏掉。显式写「今天-30 天 : 明天零点」,今天的报价也画进去。
        "defaultDataMask": {"extraFormData": {"time_range": PRICE_WINDOW_DEFAULT},
                            "filterState": {"value": PRICE_WINDOW_DEFAULT},
                            "ownState": {}},
        "description": "Price Trend only — relative to today (Last week / month / quarter / custom)",
    }
    # 声量(B/C)与广告(D/E)共用一个时间窗口,两组数据同一时间段对照着看。
    # 各图按自己的时间列截:mention_week / published_on / ad_day / last_shown。
    activity_names = [f"{prefix} · {n}" for n in
                      ("Mentions per Week", "Mentions", "Ad Impressions (est.)", "Ads")]
    activity_ids = [chart_ids[chart_names.index(n)] for n in activity_names]
    activity_window = {
        "id": f"{fid}-activity-window",
        "name": "Mentions & Ads window",
        "filterType": "filter_time",
        "type": "NATIVE_FILTER",
        "targets": [{}],
        "controlValues": {},
        "scope": {"rootPath": ["ROOT_ID"],
                  "excluded": [c for c in chart_ids if c not in activity_ids]},
        "chartsInScope": activity_ids,
        "tabsInScope": [],
        "cascadeParentIds": [],
        "defaultDataMask": {"extraFormData": {"time_range": ACTIVITY_WINDOW_DEFAULT},
                            "filterState": {"value": ACTIVITY_WINDOW_DEFAULT},
                            "ownState": {}},
        "description": "Mentions + Ads charts — relative to today, default last 12 months",
    }
    dash_body = {"dashboard_title": title, "slug": slug, "published": True,
                 "position_json": json.dumps(pos, ensure_ascii=False),
                 "json_metadata": json.dumps(
                     {"native_filter_configuration": [price_time_filter, activity_window,
                                                      kind_filter, platform_filter,
                                                      grain_filter]},
                     ensure_ascii=False)}
    if dash:
        dash_id = dash["id"]
        st, j = call("PUT", f"/api/v1/dashboard/{dash_id}", token=token, csrf=csrf, body=dash_body)
        action = "更新"
    else:
        st, j = call("POST", "/api/v1/dashboard/", token=token, csrf=csrf, body=dash_body)
        dash_id = j.get("id")
        action = "创建"
    if st not in (200, 201):
        die(f"{action}看板失败: {st} {j}")
    print(f"看板 [{title}] {action} OK id={dash_id} slug={slug}")

    for cid in chart_ids:
        call("PUT", f"/api/v1/chart/{cid}", token=token, csrf=csrf,
             body={"dashboards": [dash_id]})

    # 被改名/移除的旧图(如分线前的「CI · Mentions」)不在新清单里,删掉
    for cid in old_chart_ids - set(chart_ids):
        st, _ = call("DELETE", f"/api/v1/chart/{cid}", token=token, csrf=csrf)
        print(f"清理旧图 id={cid} -> {st}")

    print(f"完成:看板 {BASE}/superset/dashboard/{slug}/")


existing_charts = {c["slice_name"]: c["id"] for c in list_all("chart", token)}
dashboards = list_all("dashboard", token)
for cfg in DASHBOARDS:
    build_dashboard(cfg, existing_charts, dashboards)

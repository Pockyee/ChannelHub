"""Superset 一键搭建「Competitive Intelligence」竞品情报看板(幂等)。

前置:
  · db/migrations/012→013→014 已应用(存在 mart.v_ci_* 视图)
  · scripts/superset_setup.py 已跑过(存在数据源 "ChannelHub")

做四件事(全部幂等:存在则更新,不存在则创建):
  1) 注册 3 个数据集:v_ci_compare(主) / v_ci_share_of_voice / v_ci_mention_detail
  2) 建 8 张图:
       KPI 行 「自家最优价」「竞品最低价」「近 7 日提及」  大数字 ×3
       A Price Trend by Model      折线时序,按产品分组
       B Price Gap vs HUTT         折线时序,只看竞品(自家恒为 0)
       C Amazon BSR (lower=better) 折线时序 —— 唯一的销量信号
       D Mentions per Week         柱状     —— 提及量
       E Mentions                  表格     —— 全部提及,按 mention_kind 切档
                                              (test/promo/media_review/discussion)
     (图表名与指标标签一律英文:看板是给人看的对外产物,注释保持中文)
  3) 组装成看板「Competitive Intelligence」(slug=competitive-intel)
  4) 把图挂到看板;并清理被改名/移除的旧图(声明式)

口径说明见 db/migrations/014_ci_mart.sql 与 docs/COMPETITIVE_INTEL.md:
  · best_total_eur = 跨源最低到手价(售价+运费)
  · price_gap_vs_own_eur 正数 = 该款比我们贵;自家行恒为 0
  · amazon_bsr 越小越好,故 C 图 Y 轴倒置

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
DASH_SLUG = "competitive-intel"
DASH_TITLE = "Competitive Intelligence"

DATASETS = {                      # 表名 → 主时间列
    "v_ci_compare": "observed_on",
    "v_ci_share_of_voice": "mention_week",
    "v_ci_mention_detail": "published_at",
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
GAP = m("price_gap_vs_own_eur", "Price Gap vs Us (€)", agg="AVG", opt="metric_gap")
BSR = m("amazon_bsr", "Amazon BSR", agg="MIN", opt="metric_bsr")
MENTIONS_7D = m("mentions_7d", "Mentions 7d", agg="MAX", opt="metric_mentions7d")
MENTION_CNT = m("mention_cnt", "Mentions", agg="SUM", opt="metric_mention_cnt")


def flt(subject, op, comparator, name):
    return {"expressionType": "SIMPLE", "subject": subject, "operator": op,
            "comparator": comparator, "clause": "WHERE", "filterOptionName": name}


OWN_ONLY = flt("is_own", "==", True, "filter_is_own_true")
COMP_ONLY = flt("is_own", "==", False, "filter_is_own_false")


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
def chart_defs(ids):
    cmp_id = ids["v_ci_compare"]
    sov_id = ids["v_ci_share_of_voice"]
    det_id = ids["v_ci_mention_detail"]
    out = []

    def kpi(name, ds_id, metric, filters, fmt=",.2f", sub=""):
        fd = {"datasource": f"{ds_id}__table", "url_params": {},
              "viz_type": "big_number_total", "metric": metric,
              "adhoc_filters": filters, "header_font_size": 0.4,
              "subheader_font_size": 0.15, "y_axis_format": fmt,
              "subheader": sub, "time_format": "smart_date"}
        qc = query_context(ds_id, {**fd, "metrics": [metric]},
                           columns=[], metrics=[metric], orderby=[])
        out.append((name, "big_number_total", fd, qc))

    kpi("CI · Our Best Price", cmp_id, BEST_PRICE, [OWN_ONLY],
        sub="HUTT — lowest total price")
    kpi("CI · Best Competitor Price", cmp_id, BEST_PRICE, [COMP_ONLY],
        sub="ECOVACS — lowest total price")
    kpi("CI · Mentions (7 days)", cmp_id, MENTIONS_7D, [],
        fmt="SMART_NUMBER", sub="all products, all sources")

    def ts(name, ds_id, x, metric, filters, *, invert=False, series="line", fmt=",.2f"):
        fd = {"datasource": f"{ds_id}__table", "url_params": {},
              "viz_type": "echarts_timeseries_" + series,
              "x_axis": x, "granularity_sqla": x, "time_grain_sqla": None,
              "metrics": [metric], "groupby": ["display_name"],
              "adhoc_filters": filters, "row_limit": 10000,
              "x_axis_sort_asc": True, "show_legend": True,
              "markerEnabled": True, "rich_tooltip": True,
              "y_axis_format": fmt, "y_axis_reverse": invert,
              "seriesType": series}
        qc = query_context(ds_id, fd, columns=[x, "display_name"], metrics=[metric],
                           is_timeseries=True, x_axis=x, granularity=x,
                           orderby=[[x, True]])
        out.append((name, "echarts_timeseries_" + series, fd, qc))

    ts("CI · Price Trend by Model", cmp_id, "observed_on", BEST_PRICE, [])
    # 自家行恒为 0,画进去只会压平竞品曲线
    ts("CI · Price Gap vs HUTT", cmp_id, "observed_on", GAP, [COMP_ONLY])
    # BSR 越小越好 → Y 轴倒置,让「向上」= 卖得更好
    ts("CI · Amazon BSR (lower = better)", cmp_id, "observed_on", BSR, [],
       invert=True, fmt="SMART_NUMBER")
    ts("CI · Mentions per Week", sov_id, "mention_week", MENTION_CNT, [],
       series="bar", fmt="SMART_NUMBER")

    # 全量提及明细表（不再只看媒体层）
    # mention_kind 让看板一张表覆盖 test / promo / media_review / discussion，
    # 靠看板上的 native filter 切档，不必为每一档单独建图。
    # title_link 是视图里拼好的 <a>；allow_render_html 打开后才会被渲染成链接，
    # 否则 Superset 把单元格当纯文本，屏幕上就是一串 <a href=…> 源码。
    det_cols = ["published_on", "mention_kind", "display_name", "outlet", "title_link"]
    det_fd = {"datasource": f"{det_id}__table", "url_params": {},
              "viz_type": "table", "query_mode": "raw",
              "all_columns": det_cols,
              "allow_render_html": True,
              "adhoc_filters": [], "row_limit": 500,
              "order_by_cols": ['["published_on", false]'],
              "table_timestamp_format": "smart_date"}
    det_qc = query_context(det_id, det_fd, columns=det_cols,
                           metrics=[], row_limit=500,
                           orderby=[["published_on", False]])
    det_qc["queries"][0]["result_type"] = "results"
    out.append(("CI · Mentions", "table", det_fd, det_qc))
    return out


# 看板布局:每行几张图 + 行高
LAYOUT_ROWS = [
    {"charts": [0, 1, 2], "height": 40},     # KPI 行
    {"charts": [3, 4], "height": 60},        # 价格走势 + 价差
    {"charts": [5], "height": 60},           # BSR(独占一行:半宽时柱子挤成一团)
    {"charts": [6], "height": 50},           # 声量柱状图 —— 独占一行
    {"charts": [7], "height": 70},           # 全量提及明细表 —— 独占一行
]


def position_json(chart_ids, chart_names):
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASH_TITLE}},
    }
    row_ids = []
    for row_no, row in enumerate(LAYOUT_ROWS):
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

existing_charts = {c["slice_name"]: c["id"] for c in list_all("chart", token)}
chart_ids, chart_names = [], []
for name, viz, fd, qc in chart_defs(ds_ids):
    ds_id = int(fd["datasource"].split("__")[0])
    body = {"slice_name": name, "viz_type": viz,
            "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(fd, ensure_ascii=False),
            "query_context": json.dumps(qc, ensure_ascii=False)}
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

pos = position_json(chart_ids, chart_names)
dash = next((d for d in list_all("dashboard", token) if d.get("slug") == DASH_SLUG), None)
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
    "id": "NATIVE_FILTER-ci-mention-kind",
    "name": "Mention kind",
    "filterType": "filter_select",
    "type": "NATIVE_FILTER",
    "targets": [{"datasetId": ds_ids["v_ci_mention_detail"],
                 "column": {"name": "mention_kind"}}],
    "controlValues": {"multiSelect": True, "enableEmptyFilter": False,
                      "searchAllOptions": False, "inverseSelection": False},
    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
    "cascadeParentIds": [],
    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
    "description": "test / promo / media_review / discussion / other",
}
dash_body = {"dashboard_title": DASH_TITLE, "slug": DASH_SLUG, "published": True,
             "position_json": json.dumps(pos, ensure_ascii=False),
             "json_metadata": json.dumps(
                 {"native_filter_configuration": [kind_filter]}, ensure_ascii=False)}
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
print(f"看板 [{DASH_TITLE}] {action} OK id={dash_id} slug={DASH_SLUG}")

for cid in chart_ids:
    call("PUT", f"/api/v1/chart/{cid}", token=token, csrf=csrf,
         body={"dashboards": [dash_id]})

for cid in old_chart_ids - set(chart_ids):
    st, _ = call("DELETE", f"/api/v1/chart/{cid}", token=token, csrf=csrf)
    print(f"清理旧图 id={cid} -> {st}")

print(f"完成:看板 {BASE}/superset/dashboard/{DASH_SLUG}/")

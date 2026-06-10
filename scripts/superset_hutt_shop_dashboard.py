"""Superset 一键搭建「Hutt Online Shop」电商看板(幂等)。

前置:
  · db/migrations/009_mart_hutt_shop.sql 已应用(存在视图 mart.v_hutt_shop_orders)
  · scripts/superset_setup.py 已跑过(存在数据源 "ChannelHub")

做四件事(全部幂等:存在则更新,不存在则创建):
  1) 把 mart.v_hutt_shop_orders 注册成 Superset 数据集(主时间列 = order_ts)
  2) 建 8 张图:
       KPI 行  「Total Revenue」「Orders」「Avg Order Value」  大数字 ×3
       A 「Weekly Revenue & Orders」    折线时序 —— 净收入 / 订单数 按周
       B 「Revenue & Units by Product」 柱状     —— 维度=产品,净收入 / 销量
       C 「Orders by Payment Method」   饼图     —— 支付方式订单占比
       D 「Revenue by Region」          表格     —— DE 按联邦州,其他按国家
       E 「Discount Code Performance」  表格     —— 折扣码:订单/折扣额/净收入
  3) 组装成看板「Hutt Online Dashboard」(slug=hutt-online-shop)
  4) 把图挂到看板;并清理被改名/移除的旧图(声明式)

口径说明见 db/migrations/009_mart_hutt_shop.sql:
  net_total = total − refunded_amount(净收入,退款即扣)。
  当前 1 订单 = 1 行项,故 COUNT(order_name) 即订单数、AVG(net_total) 即客单价。

环境变量(从 .env 经 docker --env-file 注入,与 superset_setup.py 一致):
  SUPERSET_URL(默认 http://superset:8088)
  SUPERSET_ADMIN_USERNAME  SUPERSET_ADMIN_PASSWORD

在 docker 网络内运行:
  docker run --rm --network channelhub_channelhub \\
    --env-file .env \\
    -v "$PWD/scripts/superset_hutt_shop_dashboard.py:/dash.py:ro" \\
    prefecthq/prefect:3-latest python /dash.py
"""
import json, os, sys, http.cookiejar, urllib.request, urllib.error

# 共享 cookie jar:CSRF 是会话型,csrf_token 的 GET 会种 session cookie,
# 后续写操作必须带回同一 cookie(否则 Superset 报 "CSRF session token is missing")。
_opener = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN_USER = os.environ["SUPERSET_ADMIN_USERNAME"]
ADMIN_PW = os.environ["SUPERSET_ADMIN_PASSWORD"]
DB_NAME = "ChannelHub"
SCHEMA = "mart"
TABLE = "v_hutt_shop_orders"
DASH_SLUG = "hutt-online-shop"
DASH_TITLE = "Hutt Online Dashboard"


# ---------------------------------------------------------------------------
# HTTP 小工具(沿用 superset_setup.py 风格)
# ---------------------------------------------------------------------------
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
    """列出某类资源全部(分页),返回 result 列表。客户端匹配名字,避开 CJK 过滤编码坑。"""
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
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label,
        "optionName": opt or f"metric_{col}_{agg.lower()}",
    }


REVENUE = m("net_total", "Revenue (€)")
ORDERS = m("order_name", "Orders", agg="COUNT", opt="metric_orders")
AOV = m("net_total", "Avg Order Value (€)", agg="AVG", opt="metric_aov")
UNITS = m("quantity", "Units")
DISCOUNT = m("discount_amount", "Discount (€)")

HAS_DISCOUNT_FILTER = {  # 折扣码表只看用了码的订单
    "expressionType": "SIMPLE",
    "subject": "discount_code",
    "operator": "IS NOT NULL",
    "comparator": None,
    "clause": "WHERE",
    "filterOptionName": "filter_discount_code_not_null",
}


def query_context(ds_id, form_data, *, columns, metrics, is_timeseries=False,
                  x_axis=None, row_limit=None, orderby=None, granularity=None):
    """从 form_data 派生 query_context,使图在看板上无需手动打开即可出数。"""
    q = {
        "filters": [
            {"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
            for f in form_data.get("adhoc_filters", [])
        ],
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
    if granularity:                 # 时序图必须告知 datetime 列,否则后端报缺 dttm
        q["granularity"] = granularity
    if x_axis:
        q["x_axis"] = x_axis
    return {
        "datasource": {"id": ds_id, "type": "table"},
        "force": False,
        "queries": [q],
        "form_data": form_data,
        "result_format": "json",
        "result_type": "full",
    }


# ---------------------------------------------------------------------------
# 图定义:返回 [(名称, viz_type, form_data, query_context), ...]
# ---------------------------------------------------------------------------
def chart_defs(ds_id):
    common = {"datasource": f"{ds_id}__table", "url_params": {}}

    # KPI 大数字 ×3(整段净收入/订单数/客单价)
    def kpi(metric, fmt=",.2f"):
        fd = {**common, "viz_type": "big_number_total", "metric": metric,
              "adhoc_filters": [], "header_font_size": 0.4,
              "subheader_font_size": 0.15, "y_axis_format": fmt,
              "time_format": "smart_date"}
        qc = query_context(ds_id, {**fd, "metrics": [metric]},
                           columns=[], metrics=[metric], orderby=[])
        return fd, qc

    k1_fd, k1_qc = kpi(REVENUE)
    k2_fd, k2_qc = kpi(ORDERS, fmt="SMART_NUMBER")
    k3_fd, k3_qc = kpi(AOV)

    # A 「周净收入 & 订单」折线时序(order_week 已在视图按周截断)
    #    Mixed Chart 双 Y 轴:Query A = Revenue(主轴,左) / Query B = Orders(副轴,右),
    #    营收(€几千)和单数(个位~几十)同轴会把 Orders 压平在 0 线上。
    a_fd = {**common, "viz_type": "mixed_timeseries",
            "x_axis": "order_week", "granularity_sqla": "order_week",
            "time_grain_sqla": None,
            "metrics": [REVENUE], "groupby": [], "adhoc_filters": [],
            "seriesType": "line", "yAxisIndex": 0,
            "metrics_b": [ORDERS], "groupby_b": [], "adhoc_filters_b": [],
            "seriesTypeB": "line", "yAxisIndexB": 1,
            "y_axis_format": "SMART_NUMBER", "y_axis_format_secondary": "SMART_NUMBER",
            "row_limit": 10000, "row_limit_b": 10000, "x_axis_sort_asc": True,
            "show_legend": True, "markerEnabled": True, "rich_tooltip": True}
    a_qc = query_context(ds_id, a_fd, columns=["order_week"],
                         metrics=[REVENUE], is_timeseries=True,
                         x_axis="order_week", granularity="order_week",
                         orderby=[["order_week", True]])
    a_qc["queries"].append({**a_qc["queries"][0], "metrics": [ORDERS]})

    # B 「各产品 净收入 & 销量」柱状 —— Mixed Chart 双 Y 轴:
    #    Query A = Revenue(主轴,左) / Query B = Units(副轴,右)。
    #    金额和件数量级差 ~250 倍,同轴会把 Units 压成看不见的扁条。
    b_fd = {**common, "viz_type": "mixed_timeseries",
            "x_axis": "product_name",
            "metrics": [REVENUE], "groupby": [], "adhoc_filters": [],
            "seriesType": "bar", "yAxisIndex": 0,
            "metrics_b": [UNITS], "groupby_b": [], "adhoc_filters_b": [],
            "seriesTypeB": "bar", "yAxisIndexB": 1,
            "y_axis_format": "SMART_NUMBER", "y_axis_format_secondary": "SMART_NUMBER",
            "row_limit": 100, "row_limit_b": 100, "show_legend": True,
            "rich_tooltip": True}
    b_qc = query_context(ds_id, b_fd, columns=["product_name"],
                         metrics=[REVENUE],
                         x_axis="product_name", orderby=[[REVENUE, False]])
    # Mixed Chart 取数是两条 query:A=Revenue 之外再补 B=Units
    b_qc["queries"].append({**b_qc["queries"][0], "metrics": [UNITS],
                            "orderby": [[UNITS, False]]})

    # C 「支付方式订单占比」饼图
    c_fd = {**common, "viz_type": "pie", "groupby": ["payment_method"],
            "metric": ORDERS, "adhoc_filters": [], "row_limit": 100,
            "show_legend": True, "label_type": "key_value"}
    c_qc = query_context(ds_id, {**c_fd, "metrics": [ORDERS]},
                         columns=["payment_method"], metrics=[ORDERS],
                         orderby=[[ORDERS, False]])

    # D 「地区净收入」表格(region:DE 按联邦州,其余按国家码,见 009 迁移)
    d_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["region"], "metrics": [REVENUE, ORDERS],
            "adhoc_filters": [], "row_limit": 100,
            "order_by_cols": ['["Revenue (€)", false]']}
    d_qc = query_context(ds_id, d_fd, columns=["region"],
                         metrics=[REVENUE, ORDERS], orderby=[[REVENUE, False]])

    # E 「折扣码表现」表格(订单数/折扣总额/净收入;只看用了码的订单)
    e_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["discount_code"], "metrics": [ORDERS, DISCOUNT, REVENUE],
            "adhoc_filters": [HAS_DISCOUNT_FILTER], "row_limit": 100,
            "order_by_cols": ['["Orders", false]']}
    e_qc = query_context(ds_id, e_fd, columns=["discount_code"],
                         metrics=[ORDERS, DISCOUNT, REVENUE],
                         orderby=[[ORDERS, False]])

    return [
        ("Hutt · Total Revenue", "big_number_total", k1_fd, k1_qc),
        ("Hutt · Orders", "big_number_total", k2_fd, k2_qc),
        ("Hutt · Avg Order Value", "big_number_total", k3_fd, k3_qc),
        ("Weekly Revenue & Orders", "mixed_timeseries", a_fd, a_qc),
        ("Revenue & Units by Product", "mixed_timeseries", b_fd, b_qc),
        ("Orders by Payment Method", "pie", c_fd, c_qc),
        ("Revenue by Region", "table", d_fd, d_qc),
        ("Discount Code Performance", "table", e_fd, e_qc),
    ]


# 行布局:每行放哪几张图(按 chart_defs 序号)+ 行高。宽度 = 12 / 每行张数。
LAYOUT_ROWS = [
    {"charts": [0, 1, 2], "height": 16},   # KPI 行
    {"charts": [3, 4], "height": 50},      # 趋势 + 产品
    {"charts": [5, 6], "height": 50},      # 支付方式 + 地区
    {"charts": [7], "height": 50},         # 折扣码(整行)
]


def position_json(chart_ids, chart_names):
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID",
                      "meta": {"text": DASH_TITLE}},
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
            pos[comp_id] = {
                "type": "CHART", "id": comp_id, "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": chart_ids[idx], "uuid": None,
                         "sliceName": chart_names[idx],
                         "width": width, "height": row["height"]},
            }
    pos["GRID_ID"] = {"type": "GRID", "id": "GRID_ID",
                      "children": row_ids, "parents": ["ROOT_ID"]}
    return pos


# ===========================================================================
# 主流程
# ===========================================================================
# 1) 登录 + CSRF
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

# 2) 找数据源 ChannelHub
dbs = list_all("database", token)
db = next((d for d in dbs if d.get("database_name") == DB_NAME), None)
if not db:
    die(f"未找到数据源 [{DB_NAME}],请先跑 scripts/superset_setup.py")
db_id = db["id"]
print(f"数据源 [{DB_NAME}] id={db_id}")

# 3) 数据集 mart.v_hutt_shop_orders:存在则用之,否则创建
ds = next((d for d in list_all("dataset", token)
           if d.get("table_name") == TABLE and d.get("schema") == SCHEMA), None)
if ds:
    ds_id = ds["id"]
    print(f"数据集 {SCHEMA}.{TABLE} 已存在 id={ds_id}")
else:
    st, j = call("POST", "/api/v1/dataset/", token=token, csrf=csrf, body={
        "database": db_id, "schema": SCHEMA, "table_name": TABLE})
    if st not in (200, 201):
        die(f"创建数据集 {SCHEMA}.{TABLE} 失败: {st} {j}")
    ds_id = j["id"]
    print(f"数据集 {SCHEMA}.{TABLE} 已创建 id={ds_id}")
# 从源同步列(视图改了列才认得),并设主时间列
call("PUT", f"/api/v1/dataset/{ds_id}/refresh", token=token, csrf=csrf, body={})
call("PUT", f"/api/v1/dataset/{ds_id}", token=token, csrf=csrf,
     body={"main_dttm_col": "order_ts"})

# 4) 图:存在(同名)则更新,否则创建
existing_charts = {c["slice_name"]: c["id"] for c in list_all("chart", token)}
chart_ids, chart_names = [], []
for name, viz, fd, qc in chart_defs(ds_id):
    body = {
        "slice_name": name,
        "viz_type": viz,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(fd, ensure_ascii=False),
        "query_context": json.dumps(qc, ensure_ascii=False),
    }
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

# 5) 看板:存在(同 slug)则更新布局/标题,否则创建
pos = position_json(chart_ids, chart_names)
dash = next((d for d in list_all("dashboard", token) if d.get("slug") == DASH_SLUG), None)
# 记下旧看板里的图 id(用于步骤 7 清理被改名/移除的旧图,避免改名后残留孤图)
old_chart_ids = set()
if dash:
    _, dj = call("GET", f"/api/v1/dashboard/{dash['id']}", token=token)
    try:
        oldpos = json.loads((dj.get("result") or {}).get("position_json") or "{}")
        old_chart_ids = {
            v["meta"]["chartId"] for v in oldpos.values()
            if isinstance(v, dict) and v.get("type") == "CHART" and v.get("meta", {}).get("chartId")
        }
    except (ValueError, KeyError, TypeError):
        pass
dash_body = {
    "dashboard_title": DASH_TITLE,
    "slug": DASH_SLUG,
    "published": True,
    "position_json": json.dumps(pos, ensure_ascii=False),
}
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

# 6) 把图归属到该看板(看板上才会渲染)
for cid in chart_ids:
    call("PUT", f"/api/v1/chart/{cid}", token=token, csrf=csrf,
         body={"dashboards": [dash_id]})

# 7) 清理:旧看板上、本次已不再使用的图(改名/删图后的孤图)一并删除,
#    让"改代码即所见"——本脚本完全声明式地定义这块看板。
for cid in old_chart_ids - set(chart_ids):
    st, _ = call("DELETE", f"/api/v1/chart/{cid}", token=token, csrf=csrf)
    print(f"清理旧图 id={cid} -> {st}")

print(f"完成:看板 {BASE}/superset/dashboard/{DASH_SLUG}/")

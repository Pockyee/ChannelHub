"""Superset 一键搭建 PSI 看板(幂等)。

前置:
  · db/migrations/007_mart_psi.sql 已应用(存在视图 mart.v_psi)
  · scripts/superset_setup.py 已跑过(存在数据源 "ChannelHub")

做四件事(全部幂等:存在则更新,不存在则创建):
  1) 把 mart.v_psi 注册成 Superset 数据集(主时间列 = transaction_date)
  2) 建 4 张图:
       A 「PSI 周趋势(P/S/I)」  折线时序   —— SUM 采购/销售/库存 按周
       B 「各产品 采购 vs 销售」  柱状      —— 维度=产品,SUM 采购 / 销售
       C 「当前库存(按产品)」    饼图      —— is_latest 过滤,SUM 库存
       D 「PSI 周明细」          表格      —— 按周 SUM 采购/销售/库存
  3) 组装成看板「PSI 看板」(slug=psi),2×2 布局
  4) 把 4 张图挂到看板上

口径说明见 db/migrations/007_mart_psi.sql:
  S=门店售出, I=期末在手库存, P=I本期−I上期+S本期(库存恒等式从相邻期推出)。
  库存是存量,跨周不可加 → 产品/门店维的库存图都带 is_latest=true 过滤。

环境变量(从 .env 经 docker --env-file 注入,与 superset_setup.py 一致):
  SUPERSET_URL(默认 http://superset:8088)
  SUPERSET_ADMIN_USERNAME  SUPERSET_ADMIN_PASSWORD

在 docker 网络内运行:
  docker run --rm --network channelhub_channelhub \\
    --env-file .env \\
    -v "$PWD/scripts/superset_psi_dashboard.py:/psi.py:ro" \\
    prefecthq/prefect:3-latest python /psi.py
"""
import json, os, sys, urllib.request, urllib.error

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN_USER = os.environ["SUPERSET_ADMIN_USERNAME"]
ADMIN_PW = os.environ["SUPERSET_ADMIN_PASSWORD"]
DB_NAME = "ChannelHub"
SCHEMA = "mart"
TABLE = "v_psi"
DASH_SLUG = "psi"
DASH_TITLE = "PSI 看板"


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
        with urllib.request.urlopen(req) as r:
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
# 指标 / 过滤 / 列 构造(adhoc,免在数据集预建指标)
# ---------------------------------------------------------------------------
def m(col, label, agg="SUM"):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label,
        "optionName": f"metric_{col}_{agg.lower()}",
    }


P = m("purchase_qty", "采购 P")
S = m("sale_qty", "销售 S")
I = m("inventory_qty", "库存 I")

LATEST_FILTER = {  # is_latest = true:产品/门店维“当前库存”只取每店每品最新一期
    "expressionType": "SIMPLE",
    "subject": "is_latest",
    "operator": "==",
    "comparator": True,
    "clause": "WHERE",
    "filterOptionName": "filter_is_latest_true",
}


def query_context(ds_id, form_data, *, columns, metrics, is_timeseries=False,
                  x_axis=None, row_limit=None, orderby=None):
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
# 4 张图的 (viz_type, form_data, query_context) 定义
# ---------------------------------------------------------------------------
def chart_defs(ds_id):
    common = {"datasource": f"{ds_id}__table", "url_params": {}}

    # A 「PSI 周趋势」折线时序
    a_fd = {**common, "viz_type": "echarts_timeseries_line",
            "x_axis": "transaction_date", "time_grain_sqla": None,
            "metrics": [P, S, I], "groupby": [], "adhoc_filters": [],
            "row_limit": 10000, "x_axis_sort_asc": True,
            "show_legend": True, "markerEnabled": True}
    a_qc = query_context(ds_id, a_fd, columns=["transaction_date"],
                         metrics=[P, S, I], is_timeseries=True,
                         x_axis="transaction_date",
                         orderby=[["transaction_date", True]])

    # B 「各产品 采购 vs 销售」柱状(类别轴 = 产品)
    b_fd = {**common, "viz_type": "echarts_timeseries_bar",
            "x_axis": "product_name", "metrics": [P, S], "groupby": [],
            "adhoc_filters": [], "row_limit": 100, "show_legend": True}
    b_qc = query_context(ds_id, b_fd, columns=["product_name"], metrics=[P, S],
                         x_axis="product_name", orderby=[[S, False]])

    # C 「当前库存(按产品)」饼图,is_latest 过滤
    c_fd = {**common, "viz_type": "pie", "groupby": ["product_name"],
            "metric": I, "adhoc_filters": [LATEST_FILTER], "row_limit": 100,
            "show_legend": True, "label_type": "key_value"}
    c_qc = query_context(ds_id, {**c_fd, "metrics": [I]}, columns=["product_name"],
                         metrics=[I], orderby=[[I, False]])

    # D 「PSI 周明细」表格(按周)
    d_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["transaction_date"], "metrics": [P, S, I],
            "adhoc_filters": [], "row_limit": 1000,
            "order_by_cols": ['["transaction_date", true]']}
    d_qc = query_context(ds_id, d_fd, columns=["transaction_date"], metrics=[P, S, I],
                         orderby=[["transaction_date", True]])

    return [
        ("PSI 周趋势(P/S/I)", "echarts_timeseries_line", a_fd, a_qc),
        ("各产品 采购 vs 销售", "echarts_timeseries_bar", b_fd, b_qc),
        ("当前库存(按产品)", "pie", c_fd, c_qc),
        ("PSI 周明细", "table", d_fd, d_qc),
    ]


# ---------------------------------------------------------------------------
# 看板布局:2×2(每张宽 6 / 高 50)
# ---------------------------------------------------------------------------
def position_json(chart_ids, chart_names):
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID",
                    "children": ["ROW-1", "ROW-2"], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID",
                      "meta": {"text": DASH_TITLE}},
    }
    rows = {"ROW-1": [], "ROW-2": []}
    for idx, (cid, name) in enumerate(zip(chart_ids, chart_names)):
        row_id = "ROW-1" if idx < 2 else "ROW-2"
        comp_id = f"CHART-{idx}"
        rows[row_id].append(comp_id)
        pos[comp_id] = {
            "type": "CHART", "id": comp_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"chartId": cid, "uuid": None, "sliceName": name,
                     "width": 6, "height": 50},
        }
    for row_id in ("ROW-1", "ROW-2"):
        pos[row_id] = {
            "type": "ROW", "id": row_id, "children": rows[row_id],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
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

# 3) 数据集 mart.v_psi(存在则用之,否则创建)
dsets = list_all("dataset", token)
ds = next((d for d in dsets
           if d.get("table_name") == TABLE and (d.get("schema") == SCHEMA)), None)
if ds:
    ds_id = ds["id"]
    print(f"数据集 {SCHEMA}.{TABLE} 已存在 id={ds_id}")
else:
    st, j = call("POST", "/api/v1/dataset/", token=token, csrf=csrf, body={
        "database": db_id, "schema": SCHEMA, "table_name": TABLE})
    if st not in (200, 201):
        die(f"创建数据集失败: {st} {j}")
    ds_id = j["id"]
    print(f"数据集 {SCHEMA}.{TABLE} 已创建 id={ds_id}")
# 设主时间列为 transaction_date(时序图按周对齐)
call("PUT", f"/api/v1/dataset/{ds_id}", token=token, csrf=csrf,
     body={"main_dttm_col": "transaction_date"})

# 4) 4 张图:存在(同名)则更新,否则创建
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

# 6) 把 4 张图归属到该看板(看板上才会渲染)
for cid in chart_ids:
    call("PUT", f"/api/v1/chart/{cid}", token=token, csrf=csrf,
         body={"dashboards": [dash_id]})
print(f"完成:看板 {BASE}/superset/dashboard/{DASH_SLUG}/")

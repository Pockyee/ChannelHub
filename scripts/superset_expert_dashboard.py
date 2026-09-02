"""Superset 一键搭建 Expert PSI 看板(幂等)。

前置:
  · db/migrations/007_mart_psi.sql 已应用(存在视图 mart.v_psi)
  · scripts/superset_setup.py 已跑过(存在数据源 "ChannelHub")

做四件事(全部幂等:存在则更新,不存在则创建):
  1) 把 mart.v_psi 注册成 Superset 数据集(主时间列 = transaction_date);
     全部图共用这一个数据集 —— 原生过滤器是按列名下推的,同一数据集才不会漏图/报错
  2) 建 8 张图(产品维以"系列 Z1/Z7/X10"为上层,SKU 颜色变体为下层):
       A 「Weekly Sale & Inventory」折线时序 —— SUM 销售/库存 按周
       B 「Sale by Series」        柱状     —— 维度=系列,SUM 销售
       C 「Current Inv by Series」 饼图     —— is_latest 过滤,SUM 库存
       D 「DOS (4-week demand)」  大数字   —— 当前库存可供应天数
       E 「DOS by SKU」            表格     —— DOS / 当前库存 / 最近四周销售
       F 「Sale by Series / SKU」  表格     —— 系列→SKU 两层,SUM 销售
       G 「Sale by Bundesland」    表格     —— 按联邦州 SUM 销售 / 门店数
       H 「Sale by Shop」          表格     —— 门店销量排行:总销量 + 每 SKU 一列
  3) 组装成看板「Expert PSI Dashboard」(slug=psi),并提供筛选:
       Time range(起止日期)→ Company → SKU → Bundesland → City → PLZ → Display
  4) 把图挂到看板;并清理被改名/移除的旧图(声明式)

口径说明见 db/migrations/007_mart_psi.sql:
  S=门店售出, I=期末在手库存, P=I本期−I上期+S本期(库存恒等式从相邻期推出)。
  库存是存量,跨周不可加 → 产品/门店维的库存图都带 is_latest=true 过滤。

环境变量(从 .env 经 docker --env-file 注入,与 superset_setup.py 一致):
  SUPERSET_URL(默认 http://superset:8088)
  SUPERSET_ADMIN_USERNAME  SUPERSET_ADMIN_PASSWORD

在 docker 网络内运行:
  docker run --rm --network channelhub_channelhub \\
    --env-file .env \\
    -v "$PWD/scripts/superset_expert_dashboard.py:/dash.py:ro" \\
    prefecthq/prefect:3-latest python /dash.py
"""
import json, os, re, sys, http.cookiejar, urllib.request, urllib.error

# 共享 cookie jar:CSRF 是会话型,csrf_token 的 GET 会种 session cookie,
# 后续写操作必须带回同一 cookie(否则 Superset 报 "CSRF session token is missing")。
_opener = urllib.request.build_opener(
    urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN_USER = os.environ["SUPERSET_ADMIN_USERNAME"]
ADMIN_PW = os.environ["SUPERSET_ADMIN_PASSWORD"]
DB_NAME = "ChannelHub"
SCHEMA = "mart"
TABLE = "v_psi"                       # 含 bundesland / city / plz / display_tier 全部维度
DASH_SLUG = "psi"
DASH_TITLE = "Expert PSI Dashboard"


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
# 指标 / 过滤 / 列 构造(adhoc,免在数据集预建指标)
# ---------------------------------------------------------------------------
def m(col, label, agg="SUM", opt=None):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label,
        "optionName": opt or f"metric_{col}_{agg.lower()}",
    }


S = m("sale_qty", "Sale (S)")
I = m("inventory_qty", "Inventory (I)")
S_4W = m("sale_qty_last_4w", "Sale (last 4 weeks)", opt="metric_sale_last_4w")
# DOS = 当前库存 / 最近 4 周平均周销量 × 7 = 库存 × 28 / 最近 4 周销量。
# 此 SQL 指标仅配合 LATEST_FILTER 使用；筛选后在聚合层计算，避免平均各 SKU
# 的 DOS 而造成加权错误。销量为 0 时返回 NULL，UI 显示为空而非虚假的无限大。
DOS = {
    "expressionType": "SQL",
    "sqlExpression": "SUM(inventory_qty) * 28.0 / NULLIF(SUM(sale_qty_last_4w), 0)",
    "label": "DOS (days, 4-week demand)",
    "optionName": "metric_dos_days_4w",
    "d3format": ",.1f",
}
# Sale(售出量)：按 Bundesland 统计时也使用与其余图一致的 Sale 标签。
SALE_BY_STATE = m("sale_qty", "Sale (S)", opt="metric_sale_by_state")
SALE_BY_SHOP = m("sale_qty", "Sale (S)", opt="metric_sale_by_shop")


def sku_metrics(skus):
    """每个 SKU 一列的销量指标(条件求和),列名压到「型号 颜色」这么短。

    源串形如 "15260000419 · Imo Watch Phone Z1 green" —— 客户编码和"Imo Watch Phone"
    这种品牌前缀在表头里纯属占地方(编码在「Sale by Series / SKU」里查得到),
    所以从型号 token(Z1/Z7/X10,与 v_psi.product_series 同一套正则)截到末尾 →
    "Z1 green"。取不到型号的(将来的配件类)保留原品名,不硬猜。

    万一两个 SKU 压出同一个短名,给后来的那个补上客户编码 —— 表格列名撞车会
    让 Superset 只显示其中一列。SQL 字面量里的单引号照规矩翻倍转义。
    """
    out, used = [], set()
    for idx, sku in enumerate(skus):
        code, _, name = sku.partition(" · ")
        mm = re.search(r"([XZ]\d{1,2})\s*(.*)$", name or sku, re.I)
        label = f"{mm.group(1).upper()} {mm.group(2)}".strip() if mm else (name or sku)
        if label in used:
            label = f"{label} ({code})"
        used.add(label)
        out.append({
            "expressionType": "SQL",
            "sqlExpression":
                "SUM(CASE WHEN sku = '{}' THEN sale_qty ELSE 0 END)".format(sku.replace("'", "''")),
            "label": label,
            "optionName": f"metric_sale_sku_{idx}",
        })
    return out
STORES = m("store_id", "Stores", agg="COUNT_DISTINCT", opt="metric_stores")

# 时间过滤器的默认值。Superset 4.1 的时间控件按值猜"帧"(guessFrame):"No filter"
# 猜成 No filter 帧,点开要先在 RANGE TYPE 里选 Custom 才见到日期框;而 "具体时间 : now"
# 会被 customTimeRangeDecode 认成 Custom 帧 —— 点开直接是「起始日期 / 结束」两个选择器。
# 起点取 2020-01-01(早于本平台任何数据),终点取 now,所以默认等于全量、不会藏掉新数据。
TIME_FILTER_DEFAULT = "2020-01-01T00:00:00 : now"

# 时间范围占位:看板顶部的原生 Time range 过滤器就是靠它落到 SQL 上的 ——
# 后端 (superset/common/query_context_factory.py::_apply_filters) 把图里
# 每个 TEMPORAL_RANGE 过滤器的值换成用户选的起止区间;图里**没有**这个占位,
# 时间过滤器选了也不会作用到该图。"No filter" = 不限时间(默认全量)。
TIME_RANGE_FILTER = {
    "expressionType": "SIMPLE",
    "subject": "transaction_date",
    "operator": "TEMPORAL_RANGE",
    "comparator": "No filter",
    "clause": "WHERE",
    "filterOptionName": "filter_transaction_date_range",
}

LATEST_FILTER = {  # is_latest = true:产品/门店维“当前库存”只取每店每品最新一期
    "expressionType": "SIMPLE",
    "subject": "is_latest",
    "operator": "==",
    "comparator": True,
    "clause": "WHERE",
    "filterOptionName": "filter_is_latest_true",
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
# 7 张图的 (viz_type, form_data, query_context) 定义
# ---------------------------------------------------------------------------
# 时间范围过滤器只挂在“流量”图上(销售是流量,跨期可加)。
# 库存/DOS 三张图按 is_latest 只取每店每品的最新快照 —— is_latest 是在视图里对
# **全量**数据算的,选一个历史区间会把最新快照整个排除掉、图变空,所以这三张图
# 不带 TEMPORAL_RANGE 占位,也在下面 native_filter 里被排除出时间过滤器的范围。
SNAPSHOT_CHARTS = {
    "Current Inventory by Series",
    "DOS (days, 4-week demand)",
    "DOS by SKU (4-week demand)",
}


def chart_defs(ds_id, skus):
    common = {"datasource": f"{ds_id}__table", "url_params": {}}

    # A 「SI 周趋势」折线时序（Purchase 口径暂不展示）
    a_fd = {**common, "viz_type": "echarts_timeseries_line",
            "x_axis": "transaction_date", "granularity_sqla": "transaction_date",
            "time_grain_sqla": None,
            "metrics": [S, I], "groupby": [], "adhoc_filters": [TIME_RANGE_FILTER],
            "row_limit": 10000, "x_axis_sort_asc": True,
            "show_legend": True, "markerEnabled": True}
    a_qc = query_context(ds_id, a_fd, columns=["transaction_date"],
                         metrics=[S, I], is_timeseries=True,
                         x_axis="transaction_date", granularity="transaction_date",
                         orderby=[["transaction_date", True]])

    # B 「各系列销售」柱状(类别轴 = 系列 Z1/Z7/X10 —— SKU 之上一层)
    b_fd = {**common, "viz_type": "echarts_timeseries_bar",
            "x_axis": "product_series", "metrics": [S], "groupby": [],
            "adhoc_filters": [TIME_RANGE_FILTER], "row_limit": 100, "show_legend": True}
    b_qc = query_context(ds_id, b_fd, columns=["product_series"], metrics=[S],
                         x_axis="product_series", orderby=[[S, False]])

    # C 「当前库存(按系列)」饼图,is_latest 过滤
    c_fd = {**common, "viz_type": "pie", "groupby": ["product_series"],
            "metric": I, "adhoc_filters": [LATEST_FILTER], "row_limit": 100,
            "show_legend": True, "label_type": "key_value"}
    c_qc = query_context(ds_id, {**c_fd, "metrics": [I]}, columns=["product_series"],
                         metrics=[I], orderby=[[I, False]])

    # D 「DOS」KPI：只取每店每 SKU 的当前快照；筛选后的总库存/总需求正确加权。
    d_fd = {**common, "viz_type": "big_number_total", "metric": DOS,
            "adhoc_filters": [LATEST_FILTER], "header_font_size": 0.4,
            "subheader_font_size": 0.15, "y_axis_format": ",.1f",
            "time_format": "smart_date"}
    d_qc = query_context(ds_id, {**d_fd, "metrics": [DOS]}, columns=[],
                         metrics=[DOS], orderby=[])

    # E 「各 SKU DOS」：未选择 SKU 时可比较公司内所有 SKU；选择后则显示单 SKU。
    e_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["sku"], "metrics": [DOS, I, S_4W], "adhoc_filters": [LATEST_FILTER],
            "row_limit": 1000,
            "column_config": {"DOS (days, 4-week demand)": {"d3NumberFormat": ",.1f"}},
            "order_by_cols": ['["sku", true]']}
    e_qc = query_context(ds_id, e_fd, columns=["sku"], metrics=[DOS, I, S_4W],
                         orderby=[["sku", True]])

    # F 「销售 按系列→SKU」表格：SKU 列以编码开头，统一按编码升序。
    f_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["product_series", "sku"], "metrics": [S],
            "adhoc_filters": [TIME_RANGE_FILTER], "row_limit": 1000,
            "order_by_cols": ['["sku", true]']}
    f_qc = query_context(ds_id, f_fd, columns=["product_series", "sku"],
                         metrics=[S], orderby=[["sku", True]])

    # G 「Sale by Bundesland」表格(按德国联邦州统计 Sale)。
    # bundesland 现在是 mart.v_psi 的一列(007),不再需要单独的 v_psi_bundesland 数据集;
    # 全表同源后,门店不在 dim_store 的行也会计入((unknown) 州),与其余图总量一致。
    g_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["bundesland"], "metrics": [SALE_BY_STATE, STORES],
            "adhoc_filters": [TIME_RANGE_FILTER], "row_limit": 50,
            "order_by_cols": ['["Sale (S)", false]']}
    g_qc = query_context(ds_id, g_fd, columns=["bundesland"], metrics=[SALE_BY_STATE, STORES],
                         orderby=[[SALE_BY_STATE, False]])

    # H 「Sale by Shop」表格:一行一家门店(州/城市/邮编随行),先总销量、再每个 SKU 一列。
    # 默认按总销量降序 = 门店销量排行;普通表格,点任意列头都能改排序。
    # 分组带上 store_id:门店名在不同渠道/城市有重名,只按名字分组会把两家店并成一行。
    # 注:SKU 列是构建时按库里实际 SKU 生成的条件求和;总销量 SUM(sale_qty) 不依赖这份
    # 名单,所以万一有新 SKU 在两次 deploy 之间冒出来,总数仍然是全的,只是暂时少一列。
    h_metrics = [SALE_BY_SHOP] + sku_metrics(skus)
    h_fd = {**common, "viz_type": "table", "query_mode": "aggregate",
            "groupby": ["store_id", "store_name", "bundesland", "city", "plz"],
            "metrics": h_metrics, "adhoc_filters": [TIME_RANGE_FILTER],
            "row_limit": 1000,
            "order_by_cols": ['["Sale (S)", false]']}
    h_qc = query_context(ds_id, h_fd,
                         columns=["store_id", "store_name", "bundesland", "city", "plz"],
                         metrics=h_metrics, orderby=[[SALE_BY_SHOP, False]])

    # 每张图带 dataset id:slice 的 datasource_id 必须与图内 params/query_context 的
    # dataset 一致,否则看板渲染用 slice 的 dataset 取数 → "Columns missing in dataset"。
    # 返回顺序 = 看板上的纵向顺序(position_json 按这个顺序贪心排行)。
    return [
        ("Weekly Sale & Inventory", "echarts_timeseries_line", ds_id, a_fd, a_qc),
        ("Sale by Series", "echarts_timeseries_bar", ds_id, b_fd, b_qc),
        # 销量三张明细自上而下:门店 → 联邦州 → 系列/SKU
        ("Sale by Shop", "table", ds_id, h_fd, h_qc),
        ("Sale by Bundesland", "table", ds_id, g_fd, g_qc),
        ("Sale by Series / SKU", "table", ds_id, f_fd, f_qc),
        # 库存 / DOS 三张(按 is_latest 最新快照)
        ("Current Inventory by Series", "pie", ds_id, c_fd, c_qc),
        ("DOS (days, 4-week demand)", "big_number_total", ds_id, d_fd, d_qc),
        ("DOS by SKU (4-week demand)", "table", ds_id, e_fd, e_qc),
    ]


# ---------------------------------------------------------------------------
# 看板布局:按 chart_defs 的返回顺序自上而下排 —— 明细表(FULL_WIDTH_CHARTS)独占
# 一整行,其余图两张一行(各占宽 6);行尾落单的图铺满整行。
# 想挪图的位置,改 chart_defs 的返回顺序即可,这里不用动。
# ---------------------------------------------------------------------------
# 三张表列多,半宽会把列挤成一团,固定全宽。
FULL_WIDTH_CHARTS = {
    "Sale by Series / SKU",
    "DOS by SKU (4-week demand)",
    "Sale by Bundesland",
    "Sale by Shop",
}


def position_json(chart_ids, chart_names):
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID",
                      "meta": {"text": DASH_TITLE}},
    }
    # 先按顺序切行:全宽图自己一行,其余两张凑一行。
    rows, current = [], []
    for idx, name in enumerate(chart_names):
        if name in FULL_WIDTH_CHARTS:
            if current:
                rows.append(current); current = []
            rows.append([idx])
            continue
        current.append(idx)
        if len(current) == 2:
            rows.append(current); current = []
    if current:
        rows.append(current)

    row_ids = []
    for row_no, row in enumerate(rows):
        row_id = f"ROW-{row_no}"
        row_ids.append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [],
                       "parents": ["ROOT_ID", "GRID_ID"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for idx in row:
            comp_id = f"CHART-{idx}"
            pos[row_id]["children"].append(comp_id)
            pos[comp_id] = {
                "type": "CHART", "id": comp_id, "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"chartId": chart_ids[idx], "uuid": None,
                         "sliceName": chart_names[idx],
                         "width": 12 // len(row), "height": 50},
            }
    pos["GRID_ID"] = {"type": "GRID", "id": "GRID_ID",
                      "children": row_ids, "parents": ["ROOT_ID"]}
    return pos


def native_filter_configuration(ds_id, chart_ids, chart_names):
    """七个原生过滤器,按显示顺序:

        Time range → Company → SKU → Bundesland → City → PLZ → Display

    · Time range(filter_time):起止日期,默认值 TIME_FILTER_DEFAULT 让控件直接停在
      Custom 帧(点开就是两个日期框,不用先选 RANGE TYPE)。它不绑定某一列,而是下推一个
      time_range,由各图里的 TEMPORAL_RANGE 占位(TIME_RANGE_FILTER,列 = transaction_date)
      落到 SQL 上。**范围排除库存/DOS 三张快照图**(SNAPSHOT_CHARTS):它们按 is_latest 只
      取最新一期,选历史区间只会得到空图 —— 见 chart_defs 上方的说明。
    · Company → SKU 级联:选了公司,SKU 下拉只列该公司实际有的产品名;不选 SKU 时
      图就是该公司全部 SKU 的汇总。
    · Bundesland(mart.v_psi.bundesland,门店 PLZ 连 core.plz_bundesland,008/007):
      16 个州 + (unknown),取值少,全量预取。
    · City(mart.v_psi.city,来自 dim_store)级联在 Bundesland 之下:选了州,城市
      下拉只列该州的城市;城市有两百来个,开 searchAllOptions 支持手输搜索。
    · PLZ 级联在 Bundesland + City 之下:选了州/城市,PLZ 下拉(以及手输搜索的结果)
      只在该范围内的邮编里出。它是门店邮编(mart.v_psi.plz,来自 dim_store)。Superset 4.1 的原生过滤器
      没有自由文本输入类型,Value 过滤器开 searchAllOptions("Dynamically search all
      filter values")就是"手输邮编 → 下拉命中 → 选中"最接近的形态。注意少数 PLZ
      下有 2 家门店(201 店 / 198 个不同 PLZ),选中会同时命中它们。
    · Display 是陈列档位 Big / Small / Without Display(mart.v_psi.display_tier,
      名单见 db/seed/{big,small}_display_plz.csv)。

    七张图现在共用 mart.v_psi 一个数据集,过滤器按列名下推到范围内所有图都命中。
    """
    all_charts = list(chart_ids)
    snapshot_ids = [cid for cid, nm in zip(chart_ids, chart_names) if nm in SNAPSHOT_CHARTS]
    flow_charts = [cid for cid in chart_ids if cid not in snapshot_ids]

    def value_filter(key, name, column, *, parents=(), search_all=False):
        """Value(filter_select)过滤器,七个里有六个是这一种。"""
        return {
            "id": f"NATIVE_FILTER-{key}",
            "name": name,
            "filterType": "filter_select",
            "targets": [{"datasetId": ds_id, "column": {"name": column}}],
            "defaultDataMask": {"extraFormData": {}, "filterState": {"value": None}},
            "cascadeParentIds": list(parents),
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
            "chartsInScope": all_charts,
            "tabsInScope": [],
            "controlValues": {"enableEmptyFilter": False, "multiSelect": True,
                              "searchAllOptions": search_all, "inverseSelection": False},
        }

    return [
        {
            # 时间范围:targets 只给 datasetId(不绑列);默认 filterState 空 = 不限时间。
            "id": "NATIVE_FILTER-time_range",
            "name": "Time range",
            "filterType": "filter_time",
            "targets": [{"datasetId": ds_id}],
            "defaultDataMask": {
                "extraFormData": {"time_range": TIME_FILTER_DEFAULT},
                "filterState": {"value": TIME_FILTER_DEFAULT},
            },
            "cascadeParentIds": [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": snapshot_ids},
            "type": "NATIVE_FILTER",
            "chartsInScope": flow_charts,
            "tabsInScope": [],
            "description": "起止日期;库存 / DOS 三张图按最新快照统计,不受时间范围影响",
            "controlValues": {},
        },
        value_filter("company", "Company", "company"),
        value_filter("sku", "SKU", "product_name",
                     parents=["NATIVE_FILTER-company"]),
        value_filter("bundesland", "Bundesland", "bundesland"),
        value_filter("city", "City", "city",
                     parents=["NATIVE_FILTER-bundesland"], search_all=True),
        # searchAllOptions:下拉不预取全量,输入什么就去库里搜什么 —— 手输邮编定位单店。
        # 两个父级都要写:Superset 的级联只认**直接父级**、不传递,只挂 City 的话
        # "只选了州、没选城市"时 PLZ 就收窄不了。
        value_filter("plz", "PLZ", "plz",
                     parents=["NATIVE_FILTER-bundesland", "NATIVE_FILTER-city"],
                     search_all=True),
        # 只有三个取值,全量预取即可,不需要 searchAllOptions。
        value_filter("display_tier", "Display", "display_tier"),
    ]


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

# 3) 数据集:mart.v_psi(七张图共用);存在则用之,否则创建
def ensure_dataset(schema, table):
    ds = next((d for d in list_all("dataset", token)
               if d.get("table_name") == table and d.get("schema") == schema), None)
    if ds:
        ds_id = ds["id"]
        print(f"数据集 {schema}.{table} 已存在 id={ds_id}")
    else:
        st, j = call("POST", "/api/v1/dataset/", token=token, csrf=csrf, body={
            "database": db_id, "schema": schema, "table_name": table})
        if st not in (200, 201):
            die(f"创建数据集 {schema}.{table} 失败: {st} {j}")
        ds_id = j["id"]
        print(f"数据集 {schema}.{table} 已创建 id={ds_id}")
    # 从源同步列(视图改了列才认得;保留 is_dttm 等属性)。幂等且必须,
    # 否则按新列分组会报 "Columns missing in dataset"。
    call("PUT", f"/api/v1/dataset/{ds_id}/refresh", token=token, csrf=csrf, body={})
    # 设主时间列(时序图按周对齐;Time range 过滤器也要数据集有主时间列)
    call("PUT", f"/api/v1/dataset/{ds_id}", token=token, csrf=csrf,
         body={"main_dttm_col": "transaction_date"})
    return ds_id

ds_id = ensure_dataset(SCHEMA, TABLE)


# 3.5) 查库里实际有哪些 SKU —— 「Sale by Shop」每个 SKU 一列,列表跟着数据走,
#      不写死在脚本里(新上市的 SKU 下次跑本脚本就自动多一列)。按编码升序,
#      与「Sale by Series / SKU」「DOS by SKU」的排序口径一致。
def fetch_skus(ds_id):
    st, j = call("POST", "/api/v1/chart/data", token=token, csrf=csrf, body={
        "datasource": {"id": ds_id, "type": "table"}, "force": False,
        "result_format": "json", "result_type": "full",
        "queries": [{"columns": ["sku"], "metrics": [S], "filters": [], "extras": {},
                     "orderby": [["sku", True]], "row_limit": 1000}],
        "form_data": {"datasource": f"{ds_id}__table", "viz_type": "table"},
    })
    if st != 200:
        die(f"查 SKU 列表失败: {st} {j}")
    return [r["sku"] for r in j["result"][0]["data"] if r.get("sku")]

skus = fetch_skus(ds_id)
print(f"SKU 列表({len(skus)} 个):" + ", ".join(s.split(" · ", 1)[-1] for s in skus))

# 4) 图:存在(同名)则更新,否则创建
existing_charts = {c["slice_name"]: c["id"] for c in list_all("chart", token)}
# 图表改名时复用原 ID，保留既有 Explore/分享链接。
for old_name, new_name in {
        "SO by Bundesland": "Sale by Bundesland",
        "Expert Overall – Weekly Sale & Inventory": "Weekly Sale & Inventory"}.items():
    if new_name not in existing_charts and old_name in existing_charts:
        existing_charts[new_name] = existing_charts[old_name]
chart_ids, chart_names = [], []
for name, viz, chart_ds, fd, qc in chart_defs(ds_id, skus):
    body = {
        "slice_name": name,
        "viz_type": viz,
        "datasource_id": chart_ds,
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
    "json_metadata": json.dumps({
        "native_filter_configuration": native_filter_configuration(
            ds_id, chart_ids, chart_names),
    }, ensure_ascii=False),
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

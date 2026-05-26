"""Metabase 全自动初始化（幂等）。

建管理员 → 接 channelhub(只读角色 bi_readonly) → 建问题 → 组「ChannelHub 概览」仪表盘。
仅用标准库 urllib。按名字查重，可重复运行不重复创建。

需要的环境变量（值取自 .env，勿硬编码）：
  METABASE_URL(默认 http://metabase:3000)
  MB_ADMIN_EMAIL  MB_ADMIN_PASSWORD
  BI_READONLY_USER  BI_READONLY_PASSWORD
  PGHOST(默认 postgres)  PGDB(默认 channelhub)

在 docker 网络内运行（用 --env-file 喂真实凭据，勿手填 -e ... 占位符）：
  docker run --rm --network channelhub_channelhub \\
    --env-file .env \\
    -v "$PWD/scripts/metabase_setup.py:/mb.py:ro" \\
    prefecthq/prefect:3-latest python /mb.py
"""
import json, os, sys, time, urllib.request, urllib.error

BASE = os.environ.get("METABASE_URL", "http://metabase:3000").rstrip("/")
ADMIN_EMAIL = os.environ["MB_ADMIN_EMAIL"]
ADMIN_PW = os.environ["MB_ADMIN_PASSWORD"]
BI_USER = os.environ["BI_READONLY_USER"]
BI_PW = os.environ["BI_READONLY_PASSWORD"]
PG_HOST = os.environ.get("PGHOST", "postgres")
PG_DB = os.environ.get("PGDB", "channelhub")
DB_NAME = "ChannelHub"
DASH_NAME = "ChannelHub Overview"

# 德国 Bundesland Choropleth 自定义地图。真 PLZ 多边形在本环境不可达
# (suche-postleitzahl DNS 被屏蔽;Metabase SSRF 又拒内网自托管),改用公网可达
# 的 isellsoap/deutschlandGeoJSON 的 16 州多边形;门店 PLZ 经 mart.plz_region
# 正确上卷到 Bundesland(见迁移 007 + db/seed/load_plz_region.sh)。
# feature 属性 name 直接等于德语州名(与 plz_region.bundesland 完全一致)。
# 可经 MAP_GEOJSON_URL 覆盖(如换更高/低分辨率 geo.json,或自有公网镜像)。
GEOJSON_MAP_ID = "de_bundesland"
GEOJSON_NAME = "Germany Bundesland"
GEOJSON_URL = os.environ.get(
    "MAP_GEOJSON_URL",
    "https://raw.githubusercontent.com/isellsoap/deutschlandGeoJSON/main/2_bundeslaender/3_mittel.geo.json",
)
GEOJSON_REGION_KEY = "name"     # GeoJSON feature 属性:区域标识(德语州名)
GEOJSON_REGION_NAME = "name"    # GeoJSON feature 属性:显示名(同上)


def api(method, path, body=None, session=None):
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
    )
    req.add_header("Content-Type", "application/json")
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, raw.decode(errors="replace")


def ensure_custom_geojson(session):
    """幂等配置德国 PLZ 自定义区域地图。Metabase 会拉取并校验该 GeoJSON,
    故 Metabase 容器需能访问 GEOJSON_URL;失败则给出自托管/手动指引但不中断。"""
    st, cur = api("GET", "/api/setting/custom-geojson", session=session)
    cur = cur if isinstance(cur, dict) else {}
    # 内置地图(us_states/world_countries)只读,PUT 时必须剔除,否则整体被拒
    cur = {k: v for k, v in cur.items()
           if k not in ("us_states", "world_countries")
           and not (isinstance(v, dict) and v.get("builtin"))}
    cur[GEOJSON_MAP_ID] = {
        "name": GEOJSON_NAME, "url": GEOJSON_URL,
        "region_key": GEOJSON_REGION_KEY, "region_name": GEOJSON_REGION_NAME,
    }
    st, res = api("PUT", "/api/setting/custom-geojson", {"value": cur}, session=session)
    if st in (200, 204):
        print(f"自定义地图 [{GEOJSON_NAME}] 已配置 (id={GEOJSON_MAP_ID})")
        return True
    print(f"⚠ 配置自定义地图失败 {st}: {res}")
    print(f"  Metabase 容器需能拉取并校验 {GEOJSON_URL}")
    print("  注意:Metabase SSRF 防护只接受公网 URL,内网(MinIO/Caddy/LAN IP)会被拒。")
    print("  把 .geojson 放公网可访问处(公开 GitHub Gist/仓库 raw URL 或 jsDelivr),")
    print("  -e MAP_GEOJSON_URL='<该公网 raw URL>' 重跑;或 Admin→Settings→Maps 手动加")
    print(f"  (Region key={GEOJSON_REGION_KEY}, Region name={GEOJSON_REGION_NAME})。")
    print("  地图卡仍会建好,配好地图源即自动出图。")
    return False


# 1) 等就绪
for _ in range(60):
    try:
        st, _ = api("GET", "/api/health")
        if st == 200:
            break
    except Exception:
        pass
    time.sleep(3)
else:
    print("Metabase 未就绪"); sys.exit(1)

# 2) setup / 登录
st, props = api("GET", "/api/session/properties")
token = (props or {}).get("setup-token")
has_setup = (props or {}).get("has-user-setup")
print(f"Metabase {(props or {}).get('version', {}).get('tag', '?')} | "
      f"has-user-setup={has_setup}")

session = None
if token and not has_setup:
    st, res = api("POST", "/api/setup", {
        "token": token,
        "prefs": {"site_name": "ChannelHub", "allow_tracking": False},
        "user": {"first_name": "Admin", "last_name": "ChannelHub",
                 "email": ADMIN_EMAIL, "password": ADMIN_PW, "site_name": "ChannelHub"},
        "database": None,
    })
    if st in (200, 201) and isinstance(res, dict):
        session = res.get("id")
if not session:
    st, res = api("POST", "/api/session",
                  {"username": ADMIN_EMAIL, "password": ADMIN_PW})
    if st != 200:
        print("登录失败(可能已被初始化为别的管理员):", st, res); sys.exit(2)
    session = res["id"]
print("登录 OK")

# 2.4) 清理旧中文卡片/仪表盘(幂等)。改名后脚本按新英文名查重,旧中文对象
#      会变孤儿;此处按已知旧名删掉。先删仪表盘(解开对旧卡片的引用)再删卡片。
#      新版 Metabase 支持 DELETE;旧版回退 PUT archived=true。删干净后重跑即跳过。
LEGACY_DASHBOARDS = ["ChannelHub 概览"]
LEGACY_CARDS = [
    "当前在库总件数", "当前在库产品数(GTIN)", "当前库存 Top15 门店",
    "当前库存按运营公司", "库存随 ISO 周变化", "当前库存 Top15 产品",
    "当前快照明细", "当前库存德国地图(PLZ-2)",
    # 地图改用 Bundesland 后,旧的 PLZ-2 地图卡(英文名)也清掉,避免孤儿
    "Current Stock — Germany Map (PLZ-2)",
    # 地图卡暂时停用:重跑时删掉已建的;恢复时从这里删此行 + 取消上方 CARDS 注释
    "Current Stock — Germany Map (Bundesland)",
]


def remove(kind, oid, session):
    """删一个 card/dashboard;DELETE 不行则回退归档。返回是否成功。"""
    st, _ = api("DELETE", f"/api/{kind}/{oid}", session=session)
    if st in (200, 204):
        return True
    st, _ = api("PUT", f"/api/{kind}/{oid}", {"archived": True}, session=session)
    return st in (200, 204)


st, _dashes = api("GET", "/api/dashboard", session=session)
for d in (_dashes or []):
    if d.get("name") in LEGACY_DASHBOARDS and not d.get("archived"):
        ok = remove("dashboard", d["id"], session)
        print(f"  清理旧仪表盘 [{d['name']}] id={d['id']} "
              f"{'OK' if ok else '失败(请手动删)'}")
st, _cards = api("GET", "/api/card", session=session)
for c in (_cards or []):
    if c.get("name") in LEGACY_CARDS and not c.get("archived"):
        ok = remove("card", c["id"], session)
        print(f"  清理旧问题 [{c['name']}] id={c['id']} "
              f"{'OK' if ok else '失败(请手动删)'}")

# 2.5) 德国 PLZ 自定义区域地图(幂等;失败不中断,见函数内指引)
ensure_custom_geojson(session)

# 3) 接库(幂等)
st, dbs = api("GET", "/api/database", session=session)
db_list = dbs.get("data", dbs) if isinstance(dbs, dict) else dbs
db_id = next((d["id"] for d in (db_list or []) if d.get("name") == DB_NAME), None)
if db_id:
    print(f"数据库连接已存在 id={db_id}")
else:
    st, res = api("POST", "/api/database", {
        "name": DB_NAME, "engine": "postgres",
        "details": {"host": PG_HOST, "port": 5432, "dbname": PG_DB,
                    "user": BI_USER, "password": BI_PW, "ssl": False,
                    "tunnel-enabled": False},
        "is_full_sync": True,
    }, session=session)
    if st not in (200, 201):
        print("接库失败:", st, res); sys.exit(3)
    db_id = res["id"]
    api("POST", f"/api/database/{db_id}/sync_schema", session=session)
    print(f"已接数据库 id={db_id}")

# 4) 问题卡(幂等；读 mart 物化层；Top 卡滤掉 NULL 库存避免 NULLS FIRST 顶到首位)
#    前两张为 scalar(布局首行并排);产品口径 = 归一 GTIN;新增运营公司维
CARDS = [
    ("Total Units in Stock",
     "SELECT sum(stock_on_hand_qty) AS total FROM mart.fact_sell_through_current",
     "scalar", {}),
    ("Distinct Products in Stock (GTIN)",
     "SELECT count(DISTINCT gtin_norm) AS products FROM mart.fact_sell_through_current",
     "scalar", {}),
    # 销量看累计:必须读历史表 fact_sell_through(current 只是每店每 GTIN 最新
    # 一期的快照,sum 出来不是总销量)
    ("Total Units Sold",
     "SELECT sum(sold_qty) AS total FROM mart.fact_sell_through",
     "scalar", {}),
    # 维度按城市(JOIN dim_store);193 城≈200 店近 1:1,仅同城多店会合并
    ("Top 15 Stores by Current Stock",
     "SELECT s.city AS city, sum(f.stock_on_hand_qty) AS total "
     "FROM mart.fact_sell_through_current f "
     "JOIN mart.dim_store s USING (supplier_code, store_id) "
     "WHERE f.stock_on_hand_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
     "bar", {"graph.dimensions": ["city"], "graph.metrics": ["total"]}),
    ("Current Stock by Operating Company",
     "SELECT company, sum(stock_on_hand_qty) AS total FROM mart.fact_sell_through_current "
     "WHERE stock_on_hand_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC",
     "bar", {"graph.dimensions": ["company"], "graph.metrics": ["total"]}),
    ("Stock by ISO Week",
     "SELECT period_isoweek AS iso_week, sum(stock_on_hand_qty) AS total_stock "
     "FROM mart.fact_sell_through GROUP BY 1 ORDER BY 1",
     "bar", {"graph.dimensions": ["iso_week"], "graph.metrics": ["total_stock"]}),
    ("Top 15 Products by Current Stock",
     "SELECT p.product_name AS product, sum(f.stock_on_hand_qty) AS total "
     "FROM mart.fact_sell_through_current f "
     "JOIN mart.dim_product p USING (gtin_norm) "
     "WHERE f.stock_on_hand_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
     "bar", {"graph.dimensions": ["product"], "graph.metrics": ["total"]}),
    # --- 销量(累计,读历史表 mart.fact_sell_through;Top 卡滤 NULL 同库存口径)---
    ("Top 15 Stores by Sales",
     "SELECT s.city AS city, sum(f.sold_qty) AS total "
     "FROM mart.fact_sell_through f "
     "JOIN mart.dim_store s USING (supplier_code, store_id) "
     "WHERE f.sold_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
     "bar", {"graph.dimensions": ["city"], "graph.metrics": ["total"]}),
    ("Sales by Operating Company",
     "SELECT company, sum(sold_qty) AS total FROM mart.fact_sell_through "
     "WHERE sold_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC",
     "bar", {"graph.dimensions": ["company"], "graph.metrics": ["total"]}),
    ("Sales by ISO Week",
     "SELECT period_isoweek AS iso_week, sum(sold_qty) AS total_sold "
     "FROM mart.fact_sell_through GROUP BY 1 ORDER BY 1",
     "line", {"graph.dimensions": ["iso_week"], "graph.metrics": ["total_sold"]}),
    ("Top 15 Products by Sales",
     "SELECT p.product_name AS product, sum(f.sold_qty) AS total "
     "FROM mart.fact_sell_through f "
     "JOIN mart.dim_product p USING (gtin_norm) "
     "WHERE f.sold_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
     "bar", {"graph.dimensions": ["product"], "graph.metrics": ["total"]}),
    # LEFT JOIN:无 dim_store(空 store_id)的行仍保留,city 为空不丢明细
    ("Current Snapshot Detail",
     "SELECT s.city AS city, f.gtin_norm, f.transaction_date, "
     "f.stock_on_hand_qty, f.source_file_name "
     "FROM mart.fact_sell_through_current f "
     "LEFT JOIN mart.dim_store s USING (supplier_code, store_id) "
     "ORDER BY s.city, f.gtin_norm LIMIT 1000",
     "table", {}),
    # --- 德国地图卡【暂时停用】----------------------------------------------
    # 数据/几何都没问题(bi_readonly 查得出数、州名与 GeoJSON name 逐字一致),
    # 但 Metabase 0.61 的 region-map 可视化绑定没渲染出来。先按用户要求移除该卡
    # (下方 LEGACY_CARDS 已含此名,重跑会把已建的删掉、不再重建)。
    # 恢复:把下面这块取消注释,并从 LEGACY_CARDS 删掉同名行,重跑脚本即可。
    # mart.plz_region(迁移 007)+ 装载器 + custom-geojson 设置都保留,无需重建。
    # ("Current Stock — Germany Map (Bundesland)",
    #  "SELECT r.bundesland AS bundesland, sum(f.stock_on_hand_qty) AS total "
    #  "FROM mart.fact_sell_through_current f "
    #  "JOIN mart.dim_store s USING (supplier_code, store_id) "
    #  "JOIN mart.plz_region r "
    #  "  ON r.plz = lpad(regexp_replace(s.postal_code,'[^0-9]','','g'),5,'0') "
    #  "WHERE f.stock_on_hand_qty IS NOT NULL "
    #  "GROUP BY 1 ORDER BY 2 DESC",
    #  "map", {"map.type": "region", "map.region": GEOJSON_MAP_ID,
    #          "map.metric_column": "total", "map.dimension_column": "bundesland",
    #          "map.metric": ["total"], "map.dimension": ["bundesland"]}),
]

st, existing = api("GET", "/api/card", session=session)
by_name = {c["name"]: c["id"] for c in (existing or [])}
card_meta = []  # [(card_id, display)] —— display 决定仪表盘分区(scalar 顶行)
for name, sql, disp, viz in CARDS:
    if name in by_name:
        cid = by_name[name]
        api("PUT", f"/api/card/{cid}", {
            "dataset_query": {"type": "native", "database": db_id,
                              "native": {"query": sql, "template-tags": {}}},
            "display": disp, "visualization_settings": viz,
        }, session=session)
        created = False
    else:
        st, res = api("POST", "/api/card", {
            "name": name, "display": disp,
            "dataset_query": {"type": "native", "database": db_id,
                              "native": {"query": sql, "template-tags": {}}},
            "visualization_settings": viz,
        }, session=session)
        if st not in (200, 201):
            print(f"  建问题失败 [{name}]: {st} {res}"); continue
        cid, created = res["id"], True
    card_meta.append((cid, disp))
    print(f"  问题 [{name}] id={cid} {'(新建)' if created else '(更新)'}")

# 5) 仪表盘(幂等) + 布局
st, dashes = api("GET", "/api/dashboard", session=session)
dash_id = next((d["id"] for d in (dashes or []) if d.get("name") == DASH_NAME), None)
if not dash_id:
    st, res = api("POST", "/api/dashboard",
                  {"name": DASH_NAME,
                   "description": "Inventory & sales overview (auto-generated)"},
                  session=session)
    dash_id = res["id"]
    print(f"已建仪表盘 id={dash_id}")
else:
    print(f"仪表盘已存在 id={dash_id}")

# 布局按显示类型自动分区(不再硬编码"前 N 张是 scalar"):所有 scalar 卡
# 并排顶部(每行 3 张,各 6 宽 3 高),其余大卡(bar/line/table/map)各占整行
# (18 宽 6 高)堆其下。新增/删卡不破布局。
SCALAR_PER_ROW = 3
scalars = [cid for cid, d in card_meta if d == "scalar"]
others = [cid for cid, d in card_meta if d != "scalar"]
dashcards = []
nid = -1
for k, cid in enumerate(scalars):
    dashcards.append({"id": nid, "card_id": cid,
                      "row": (k // SCALAR_PER_ROW) * 3,
                      "col": (k % SCALAR_PER_ROW) * 6, "size_x": 6, "size_y": 3})
    nid -= 1
base = ((len(scalars) + SCALAR_PER_ROW - 1) // SCALAR_PER_ROW) * 3
for j, cid in enumerate(others):
    dashcards.append({"id": nid, "card_id": cid, "row": base + j * 6,
                      "col": 0, "size_x": 18, "size_y": 6})
    nid -= 1
st, _ = api("PUT", f"/api/dashboard/{dash_id}",
            {"dashcards": dashcards}, session=session)
if st == 200:
    print(f"仪表盘已布置 {len(dashcards)} 张卡片")
else:
    ok = sum(api("POST", f"/api/dashboard/{dash_id}/cards",
                 {"cardId": dc["card_id"], "row": dc["row"], "col": dc["col"],
                  "size_x": dc["size_x"], "size_y": dc["size_y"]},
                 session=session)[0] in (200, 201) for dc in dashcards)
    print(f"仪表盘布置(回退) {ok}/{len(dashcards)}")

print(f"\nDONE → 本机访问 http://localhost:3000/dashboard/{dash_id}")

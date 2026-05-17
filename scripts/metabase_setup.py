"""Metabase 全自动初始化（幂等）。

建管理员 → 接 channelhub(只读角色 bi_readonly) → 建问题 → 组「ChannelHub 概览」仪表盘。
仅用标准库 urllib。按名字查重，可重复运行不重复创建。

需要的环境变量（值取自 .env，勿硬编码）：
  METABASE_URL(默认 http://metabase:3000)
  MB_ADMIN_EMAIL  MB_ADMIN_PASSWORD
  BI_READONLY_USER  BI_READONLY_PASSWORD
  PGHOST(默认 postgres)  PGDB(默认 channelhub)

在 docker 网络内运行：
  docker run --rm --network channelhub_channelhub \\
    -e MB_ADMIN_EMAIL=... -e MB_ADMIN_PASSWORD=... \\
    -e BI_READONLY_USER=... -e BI_READONLY_PASSWORD=... \\
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
DASH_NAME = "ChannelHub 概览"


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

# 4) 问题卡(幂等；Top 卡滤掉 NULL 库存避免 NULLS FIRST 顶到首位)
CARDS = [
    ("当前在库总件数",
     "SELECT sum(stock_on_hand_qty) AS total FROM core.fact_sell_through_current",
     "scalar", {}),
    ("当前在库 SKU 数",
     "SELECT count(DISTINCT customer_sku_code) AS skus FROM core.fact_sell_through_current",
     "scalar", {}),
    ("当前库存 Top15 门店",
     "SELECT store_id, sum(stock_on_hand_qty) AS total FROM core.fact_sell_through_current "
     "WHERE stock_on_hand_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
     "bar", {"graph.dimensions": ["store_id"], "graph.metrics": ["total"]}),
    ("库存随 ISO 周变化",
     "SELECT period_isoweek AS kw, sum(stock_on_hand_qty) AS total_stock "
     "FROM core.fact_sell_through GROUP BY 1 ORDER BY 1",
     "bar", {"graph.dimensions": ["kw"], "graph.metrics": ["total_stock"]}),
    ("当前库存 Top15 SKU",
     "SELECT s.customer_sku_name AS sku, sum(f.stock_on_hand_qty) AS total "
     "FROM core.fact_sell_through_current f "
     "JOIN core.dim_sku s USING (supplier_code, customer_sku_code) "
     "WHERE f.stock_on_hand_qty IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
     "bar", {"graph.dimensions": ["sku"], "graph.metrics": ["total"]}),
    ("当前快照明细",
     "SELECT store_id, customer_sku_code, transaction_date, stock_on_hand_qty, "
     "source_file_name FROM core.fact_sell_through_current "
     "ORDER BY store_id, customer_sku_code LIMIT 1000",
     "table", {}),
]

st, existing = api("GET", "/api/card", session=session)
by_name = {c["name"]: c["id"] for c in (existing or [])}
card_ids = []
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
    card_ids.append(cid)
    print(f"  问题 [{name}] id={cid} {'(新建)' if created else '(更新)'}")

# 5) 仪表盘(幂等) + 布局
st, dashes = api("GET", "/api/dashboard", session=session)
dash_id = next((d["id"] for d in (dashes or []) if d.get("name") == DASH_NAME), None)
if not dash_id:
    st, res = api("POST", "/api/dashboard",
                  {"name": DASH_NAME, "description": "库存概览(自动生成)"},
                  session=session)
    dash_id = res["id"]
    print(f"已建仪表盘 id={dash_id}")
else:
    print(f"仪表盘已存在 id={dash_id}")

dashcards = []
for i, cid in enumerate(card_ids):
    if i < 2:  # 两个 scalar 并排首行
        dashcards.append({"id": -1 - i, "card_id": cid, "row": 0,
                          "col": i * 6, "size_x": 6, "size_y": 3})
    else:      # 其余每行一张大卡
        dashcards.append({"id": -1 - i, "card_id": cid, "row": 3 + (i - 2) * 6,
                          "col": 0, "size_x": 18, "size_y": 6})
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

"""Superset 注册 ChannelHub 数据源(幂等)。

只做一件事:用 admin 凭据登录,确认/创建 "ChannelHub" Postgres 数据源
(用 bi_readonly 只读账号连同实例的 channelhub 库)。
不预建图表与仪表盘 — 那些由人 + Claude Code 按需在 UI 或 API 增量构建。

需要的环境变量(从 .env 经 docker --env-file 注入):
  SUPERSET_URL(默认 http://superset:8088)
  SUPERSET_ADMIN_USERNAME  SUPERSET_ADMIN_PASSWORD
  BI_READONLY_USER  BI_READONLY_PASSWORD
  PGHOST(默认 postgres)  PGDB(默认 channelhub)

在 docker 网络内运行:
  docker run --rm --network channelhub_channelhub \\
    --env-file .env \\
    -v "$PWD/scripts/superset_setup.py:/ss.py:ro" \\
    prefecthq/prefect:3-latest python /ss.py
"""
import json, os, sys, urllib.request, urllib.error

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
ADMIN_USER = os.environ["SUPERSET_ADMIN_USERNAME"]
ADMIN_PW = os.environ["SUPERSET_ADMIN_PASSWORD"]
BI_USER = os.environ["BI_READONLY_USER"]
BI_PW = os.environ["BI_READONLY_PASSWORD"]
PG_HOST = os.environ.get("PGHOST", "postgres")
PG_DB = os.environ.get("PGDB", "channelhub")
DB_NAME = "ChannelHub"


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


# 1) 登录拿 access_token
st, j = call("POST", "/api/v1/security/login", body={
    "username": ADMIN_USER, "password": ADMIN_PW,
    "provider": "db", "refresh": False,
})
if st != 200 or "access_token" not in j:
    print(f"登录失败: {st} {j}", file=sys.stderr)
    sys.exit(1)
token = j["access_token"]
print("登录 OK")

# 2) 拿 CSRF token(写操作必带)
st, j = call("GET", "/api/v1/security/csrf_token/", token=token)
if st != 200:
    print(f"获取 CSRF 失败: {st} {j}", file=sys.stderr)
    sys.exit(1)
csrf = j["result"]

# 3) 查 ChannelHub 数据源在不在
st, j = call("GET",
             "/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:" + DB_NAME + ")))",
             token=token)
existing = (j.get("result") or []) if st == 200 else []
if existing:
    print(f"数据源 [{DB_NAME}] 已存在 id={existing[0]['id']},跳过创建。")
    sys.exit(0)

# 4) 创建数据源
uri = f"postgresql+psycopg2://{BI_USER}:{BI_PW}@{PG_HOST}:5432/{PG_DB}"
st, j = call("POST", "/api/v1/database/", token=token, csrf=csrf, body={
    "engine": "postgresql",
    "configuration_method": "sqlalchemy_form",
    "database_name": DB_NAME,
    "sqlalchemy_uri": uri,
    "expose_in_sqllab": True,
    "allow_ctas": False,
    "allow_cvas": False,
    "allow_dml": False,
})
if st in (200, 201):
    print(f"数据源 [{DB_NAME}] 已创建 id={j.get('id')}")
else:
    print(f"创建数据源失败: {st} {j}", file=sys.stderr)
    sys.exit(1)

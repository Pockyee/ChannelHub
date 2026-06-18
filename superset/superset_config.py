"""Superset 配置（挂载到容器 /app/pythonpath/superset_config.py）。

仅做最少配置:
- 元数据库走同一个 Postgres 实例的 superset 库（避免再起一个 sqlite 文件）
- Caddy HTTPS 反代后面跑,启 ProxyFix 让 Superset 看见真实 scheme/host
"""
import os

# 会话签名 / 加密 — 必须稳定;改了之后已有会话失效、缓存失效。
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# 元数据库（与 channelhub/metabase/prefect 共享同一个 Postgres 实例）
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    "@postgres:5432/superset"
)

# 在 Caddy 反代后面运行 — 让 Superset 信任 X-Forwarded-* 头
ENABLE_PROXY_FIX = True
PREFERRED_URL_SCHEME = "https"

# 关掉 Superset 自带的示例数据;我们只关心 ChannelHub 这个数据源
SUPERSET_LOAD_EXAMPLES = False

FEATURE_FLAGS = {
    # SQL Lab 模板渲染（{{ ... }} 参数化查询）
    "ENABLE_TEMPLATE_PROCESSING": True,
    # 仪表盘级 RBAC — 给业务同事按 dashboard 粒度授权
    "DASHBOARD_RBAC": True,
}

# 后台 cron 类任务我们暂时不开（不跑 celery worker）
CELERY_CONFIG = None

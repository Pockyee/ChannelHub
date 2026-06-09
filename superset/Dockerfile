# Superset + Postgres 驱动 + ChannelHub 数据源驱动
#
# 上游 apache/superset 镜像只内置 SQLite 驱动;元数据库要用 Postgres(本项目),
# 业务数据源也是 Postgres,都需要 psycopg2。这里只加一层 pip install,镜像
# 仍以官方 4.1.1 为基础。
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
USER superset

#!/usr/bin/env bash
# 一次性初始化：把 docker compose 已经拉起的空栈,变成可用状态。
#
# 做以下步骤(全部幂等,可重复跑):
#   1) 应用 db/migrations/*.sql                                → raw/core/mart schema
#   2) 设 bi_readonly 角色密码(取自 .env)
#   3) 装载 db/seed/gtin_whitelist.csv                          → core.gtin_whitelist
#   4) 在 Postgres 建空的 superset 元数据库(若不存在)
#   5) Metabase 自动化(管理员/接 channelhub 数据源/建仪表盘) — 走 SSH 隧道访问
#   6) Superset 注册 ChannelHub 数据源                          — 主对外 BI
#   7) 应用 007 PSI 口径视图 mart.v_psi + 建 Expert PSI 看板    — 主对外 BI
#
# 前置:
#   - docker compose up -d 已跑过,postgres/metabase/superset/superset-init 都健康
#   - 项目根目录有 .env(权限 600),所有 *_PASSWORD / *_ADMIN_* / POSTGRES_* 已填
#
# 用法(在项目根目录执行,或 sudo -u deploy bash -lc):
#   bash scripts/initialize.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ ! -f .env ]]; then
  echo "✗ 未找到 .env(应在 $REPO_ROOT/.env)" >&2
  exit 1
fi

# 复用 .env 里的 PG 账户(默认 channelhub/channelhub)
PG_USER=$(grep -E '^POSTGRES_USER=' .env | cut -d= -f2-)
PG_DB=$(grep -E '^POSTGRES_DB=' .env | cut -d= -f2-)
PG_USER="${PG_USER:-channelhub}"
PG_DB="${PG_DB:-channelhub}"

# compose 网络名;docker-compose.yml 顶部 name: channelhub → 网络为 channelhub_channelhub
NET=channelhub_channelhub
if ! docker network inspect "$NET" >/dev/null 2>&1; then
  NET=$(docker network ls --format '{{.Name}}' | grep '_channelhub$' | head -1 || true)
fi
if [[ -z "$NET" ]]; then
  echo "✗ 找不到 channelhub compose 网络,docker compose ps 看看栈起没起。" >&2
  exit 1
fi

echo "==> 1/6 应用 db/migrations 全部 .sql"
# 这些迁移设计为"前向应用一次":002/003/005 都重塑同一组视图,旧版无法
# CREATE OR REPLACE 到新结构。检测到 core.gtin_whitelist 存在即认为已初始化,跳过。
# 后续新增迁移(如 007+)请手动 psql 应用,或在此脚本里专门加一步。
ALREADY=$(docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -tAc \
  "SELECT 1 FROM information_schema.tables WHERE table_schema='core' AND table_name='gtin_whitelist'" \
  2>/dev/null | tr -d '[:space:]')
if [[ "$ALREADY" == "1" ]]; then
  echo "  · 检测到 core.gtin_whitelist,数据层已初始化,跳过迁移。"
else
  shopt -s nullglob
  migrations=(db/migrations/*.sql)
  shopt -u nullglob
  if (( ${#migrations[@]} == 0 )); then
    echo "  · 无迁移文件,跳过"
  else
    for f in "${migrations[@]}"; do
      echo "  · $(basename "$f")"
      docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" \
        -v ON_ERROR_STOP=1 -f - < "$f" >/dev/null
    done
  fi
fi

echo "==> 2/6 设 bi_readonly 角色密码(取自 .env)"
BI_PWD=$(grep -E '^BI_READONLY_PASSWORD=' .env | cut -d= -f2-)
if [[ -z "$BI_PWD" ]]; then
  echo "✗ .env 里 BI_READONLY_PASSWORD 为空,无法继续。" >&2
  exit 1
fi
# 用 stdin,因为 psql 的 :'var' 替换只在 stdin/文件读时生效(-c 不做)。
docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" \
  -v ON_ERROR_STOP=1 -v pw="$BI_PWD" <<'SQL' >/dev/null
ALTER ROLE bi_readonly PASSWORD :'pw';
SQL
echo "  · 已设置"

echo "==> 3/6 装载 GTIN 白名单 seed"
bash db/seed/load_gtin_whitelist.sh

echo "==> 4/6 建 Postgres 中的 superset 元数据库(若不存在)"
HAS_SS=$(docker compose exec -T postgres psql -U "$PG_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname='superset'" 2>/dev/null | tr -d '[:space:]')
if [[ "$HAS_SS" == "1" ]]; then
  echo "  · superset 库已存在"
else
  docker compose exec -T postgres psql -U "$PG_USER" -d postgres \
    -c "CREATE DATABASE superset" >/dev/null
  echo "  · 已建 superset 空库"
fi

echo "==> 5/6 Metabase 自动化(管理员/数据源/仪表盘) — 经 SSH 隧道用"
docker run --rm --network "$NET" \
  --env-file .env \
  -v "$REPO_ROOT/scripts/metabase_setup.py:/mb.py:ro" \
  prefecthq/prefect:3-latest python /mb.py

echo "==> 6/7 Superset 注册 ChannelHub 数据源 — 主对外 BI(443)"
docker run --rm --network "$NET" \
  --env-file .env \
  -v "$REPO_ROOT/scripts/superset_setup.py:/ss.py:ro" \
  prefecthq/prefect:3-latest python /ss.py

echo "==> 7/7 Superset BI 供给(口径视图 + 看板,幂等)"
# 单一事实源:与 deploy.yml 调的是同一个脚本(应用 BI 视图 + 确保 admin/数据源 +
# 重跑所有 superset_*_dashboard.py)。本地改看板代码 push 后,deploy 会自动重放。
bash scripts/superset_provision.sh

echo
echo "✓ 初始化完成。"
echo "  · Superset(主 BI):浏览器开 https://<服务器IP> 用 SUPERSET_ADMIN_USERNAME/PASSWORD 登录"
echo "  · Expert PSI 看板:https://<服务器IP>/superset/dashboard/psi/"
echo "  · Metabase(备用): SSH 隧道 ssh -L 3000:localhost:3000 deploy@<服务器IP> → http://localhost:3000"
echo "  · 任一步失败,修复后重跑本脚本即可(全部幂等)。"

#!/usr/bin/env bash
# ============================================================================
# Superset BI 供给(幂等):把"用代码定义的看板呈现"重放到 Superset。
# ----------------------------------------------------------------------------
# 目的:本地改 scripts/superset_*_dashboard.py(图、布局、标题…)→ git push →
#       deploy 自动调用本脚本 → 服务器看板即时更新,**无需上服务器手动跑任何东西**。
#
# 做四件事,全部幂等(可重复跑,存在则更新):
#   1) 等 Superset 就绪(/health)
#   2) 应用 BI 口径视图(CREATE OR REPLACE,见下方 BI_VIEW_MIGRATIONS)
#   3) 确保 Superset admin 存在(superset-init 的 create-admin 有时没建成)
#      + 确保 ChannelHub 数据源(scripts/superset_setup.py)
#   4) 重跑所有看板构建脚本 scripts/superset_*_dashboard.py(create-or-update)
#
# 被谁调用:
#   · .github/workflows/deploy.yml —— 每次部署 docker compose up 之后自动跑
#   · scripts/initialize.sh        —— 首次初始化时复用(单一事实源)
#   · 也可本地/服务器手动:bash scripts/superset_provision.sh
#
# 前置:docker compose 栈已起(postgres/superset 健康),项目根有 .env。
# ============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# 只挑"幂等可重放"的 BI 视图迁移(CREATE OR REPLACE VIEW)。**别**把会 DROP CASCADE
# 重塑 core 视图链的 002/003/005 放进来——那些由 initialize.sh 一次性前向应用。
# 以后新增同类 BI 视图(如 008_mart_xxx.sql 也是 CREATE OR REPLACE),加到这里即可。
BI_VIEW_MIGRATIONS=(
  db/migrations/007_mart_psi.sql
  db/migrations/008_geo_plz_bundesland.sql
  db/migrations/009_mart_hutt_shop.sql
)

# 幂等可重放的 BI 参照 seed(CSV 即权威,loader 内 TRUNCATE+\copy+sync)。
# 这些是 BI 视图的数据依赖(如 v_psi_bundesland 需要 PLZ→州参照),每次 deploy 重放。
BI_SEED_LOADERS=(
  db/seed/load_plz_bundesland.sh
)

if [[ ! -f .env ]]; then
  echo "✗ 未找到 .env(应在 $REPO_ROOT/.env)" >&2
  exit 1
fi

envval() { grep -E "^$1=" .env | cut -d= -f2- | head -1; }
PG_USER="$(envval POSTGRES_USER)"; PG_USER="${PG_USER:-channelhub}"
PG_DB="$(envval POSTGRES_DB)";     PG_DB="${PG_DB:-channelhub}"

# compose 网络名:docker-compose.yml 顶部 name: channelhub → channelhub_channelhub
NET=channelhub_channelhub
if ! docker network inspect "$NET" >/dev/null 2>&1; then
  NET=$(docker network ls --format '{{.Name}}' | grep '_channelhub$' | head -1 || true)
fi
[[ -n "$NET" ]] || { echo "✗ 找不到 channelhub compose 网络,栈起了吗?" >&2; exit 1; }

# --- 1) 等 Superset 就绪(镜像内置 curl,见 compose healthcheck)----------------
echo "==> [1/4] 等 Superset 就绪"
ready=
for i in $(seq 1 40); do            # 最多 ~120s
  if docker compose exec -T superset curl -fsS http://localhost:8088/health >/dev/null 2>&1; then
    ready=1; echo "  · Superset healthy ($i)"; break
  fi
  sleep 3
done
[[ -n "$ready" ]] || { echo "✗ Superset 120s 内未就绪" >&2; docker compose logs --tail=60 superset >&2 || true; exit 1; }

# --- 2) 应用 BI 口径视图(幂等)+ 装载 BI 参照 seed -----------------------------
echo "==> [2/4] 应用 BI 口径视图 + 参照 seed"
export POSTGRES_USER="$PG_USER" POSTGRES_DB="$PG_DB"   # 供 loader 内 docker compose exec 用
for f in "${BI_VIEW_MIGRATIONS[@]}"; do
  echo "  · 视图 $(basename "$f")"
  docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" \
    -v ON_ERROR_STOP=1 -f - < "$f" >/dev/null
done
for s in "${BI_SEED_LOADERS[@]}"; do
  echo "  · seed $(basename "$s")"
  bash "$s" >/dev/null
done

# --- 3) 确保 admin + 数据源 ---------------------------------------------------
echo "==> [3/4] 确保 Superset admin + ChannelHub 数据源"
SS_USER="$(envval SUPERSET_ADMIN_USERNAME)"; SS_USER="${SS_USER:-admin}"
SS_PW="$(envval SUPERSET_ADMIN_PASSWORD)"
SS_EMAIL="$(envval SUPERSET_ADMIN_EMAIL)"; SS_EMAIL="${SS_EMAIL:-admin@channelhub.local}"
# create-admin 幂等:用户已存在 → 报错但不改密码;用 || true 兜住,不让部署失败。
# (用户不存在才真正创建;-T 无 TTY + 全参数 → 非交互,绕开 superset-init 的交互坑)
docker compose exec -T superset superset fab create-admin \
  --username "$SS_USER" --firstname Admin --lastname User \
  --email "$SS_EMAIL" --password "$SS_PW" >/dev/null 2>&1 || true
echo "  · 已确保 admin [$SS_USER](不存在则建,存在则不动密码)"
docker run --rm --network "$NET" --env-file .env \
  -v "$REPO_ROOT/scripts/superset_setup.py:/ss.py:ro" \
  prefecthq/prefect:3-latest python /ss.py

# --- 4) 重跑所有看板构建脚本(create-or-update)-------------------------------
echo "==> [4/4] 构建/更新看板"
shopt -s nullglob
builders=(scripts/superset_*_dashboard.py)
shopt -u nullglob
(( ${#builders[@]} )) || { echo "  · 无 superset_*_dashboard.py,跳过"; }
for s in "${builders[@]}"; do
  echo "  --- $(basename "$s") ---"
  docker run --rm --network "$NET" --env-file .env \
    -v "$REPO_ROOT/$s:/dash.py:ro" \
    prefecthq/prefect:3-latest python /dash.py
done

echo "✓ BI 供给完成。"

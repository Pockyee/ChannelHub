#!/usr/bin/env bash
# 一次性初始化：把 docker compose 已经拉起的空栈,变成可用状态。
#
# 做四件事(全部幂等,可重复跑):
#   1) 按编号顺序应用 db/migrations/*.sql  → 建 raw/core/mart schema 与对象
#   2) 用 .env 里 BI_READONLY_PASSWORD 设 bi_readonly 角色密码
#   3) 装载 db/seed/gtin_whitelist.csv 到 core.gtin_whitelist
#   4) 跑 scripts/metabase_setup.py: 建 MB 管理员/接 channelhub 数据源/建仪表盘
#
# 前置:
#   - docker compose up -d 已跑过,postgres / metabase 都 healthy
#   - 项目根目录有 .env(权限 600),且 BI_READONLY_PASSWORD / MB_ADMIN_* / POSTGRES_* 已填
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

echo "==> 1/4 应用 db/migrations 全部 .sql"
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

echo "==> 2/4 设 bi_readonly 角色密码(取自 .env)"
BI_PWD=$(grep -E '^BI_READONLY_PASSWORD=' .env | cut -d= -f2-)
if [[ -z "$BI_PWD" ]]; then
  echo "✗ .env 里 BI_READONLY_PASSWORD 为空,无法继续。" >&2
  exit 1
fi
docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" \
  -v pw="$BI_PWD" -c "ALTER ROLE bi_readonly PASSWORD :'pw'" >/dev/null
echo "  · 已设置"

echo "==> 3/4 装载 GTIN 白名单 seed"
bash db/seed/load_gtin_whitelist.sh

echo "==> 4/4 Metabase 自动化(管理员/数据源/仪表盘)"
# 同 compose 网络;docker-compose.yml 顶部 name: channelhub → 网络为 channelhub_channelhub
NET=channelhub_channelhub
if ! docker network inspect "$NET" >/dev/null 2>&1; then
  # 兼容自定义项目名:取第一个名字带 _channelhub 后缀的网络
  NET=$(docker network ls --format '{{.Name}}' | grep '_channelhub$' | head -1 || true)
fi
if [[ -z "$NET" ]]; then
  echo "✗ 找不到 channelhub compose 网络,docker compose ps 看看栈起没起。" >&2
  exit 1
fi

docker run --rm --network "$NET" \
  --env-file .env \
  -v "$REPO_ROOT/scripts/metabase_setup.py:/mb.py:ro" \
  prefecthq/prefect:3-latest python /mb.py

echo
echo "✓ 初始化完成。浏览器开 https://<服务器IP> 用 MB_ADMIN_EMAIL/PASSWORD 登录 Metabase。"
echo "  如脚本中某步失败,修复后重跑本脚本即可(全部幂等)。"

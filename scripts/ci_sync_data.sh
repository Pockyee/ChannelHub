#!/usr/bin/env bash
# ============================================================================
# 竞品情报(CI)数据同步:把一台机器 Postgres 里的 raw.ci_* 事实表搬到另一台。
# ----------------------------------------------------------------------------
# 典型场景:本地用家用宽带跑了几周 ci-price,价格历史比云端完整;云端刚开采集
#          (或者一直在干跑)几乎没数据 —— 把本地历史推上去,云端曲线立刻完整。
#
# 两个子命令,都只操作**本机** docker compose 栈里的 Postgres:
#   export            把本机 raw.ci_* 数据导成一份自包含的 SQL 流(stdout)
#   import <file|->   把 export 产物灌进本机 Postgres(幂等,撞唯一键即跳过)
#
# 本地 → 云端,一条管道即可(在本地执行,不落临时文件):
#   bash scripts/ci_sync_data.sh export \
#     | ssh deploy@<服务器> 'cd /opt/channelhub && bash scripts/ci_sync_data.sh import -'
# 或分两步:export > ci_data.sql;scp 上去;服务器上 import ci_data.sql。
#
# 幂等依据(见 db/migrations/013_ci_raw.sql 各表 UNIQUE 约束):
#   ci_offer         (source_code, product_id, merchant_name, observed_on)
#   ci_listing_stat  (source_code, product_id, observed_on)
#   ci_mention       (source_code, external_id, product_id)
#   ci_unmatched     (source_code, external_id)
#   ci_ad            (source_code, ad_id)       —— 广告层(019);目标端需已应用 019
# 两边都已存在的行以**目标端为准**(ON CONFLICT DO NOTHING),绝不原地覆盖 ——
# raw 层是 append-only 历史留痕,同步只补缺不改旧。整份导入跑在一个事务里,
# 任一表出错整笔回滚,不会出现"价格进了、提及没进"的半套状态。
#
# 刻意不搬的东西:
#   · 自增主键(offer_id 等)与 total_cents 生成列 —— 目标端自己算
#   · snapshot_id —— 指向源端 raw.ci_snapshot 的自增 id,目标端对不上,置 NULL。
#     所有 mart.v_ci_* 视图都不用它;只有「回溯重解析原始页面」才需要,那要连
#     MinIO 的 ci-archive 桶一起 mc mirror,不在本脚本范围
#   · raw.ci_snapshot / MinIO 存档 / core.ci_hashtag 配额账本 / mart.ci_digest 简报
#
# 前置:两端 docker compose 栈都在跑,项目根有 .env(取 POSTGRES_USER/DB);
#       目标端已应用 012/013 迁移(部署时 superset_provision.sh 会做)。
# 可选环境变量:PG_CONTAINER(默认 channelhub-postgres)、PG_DB_OVERRIDE(导入到
#       另一个库,仅用于演练/测试)。
# ============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TABLES=(ci_offer ci_listing_stat ci_mention ci_unmatched ci_ad)

envval() { grep -E "^$1=" .env 2>/dev/null | cut -d= -f2- | head -1; }
PG_USER="$(envval POSTGRES_USER)"; PG_USER="${PG_USER:-channelhub}"
PG_DB="$(envval POSTGRES_DB)";     PG_DB="${PG_DB:-channelhub}"

# 直接 docker exec 固定容器名(docker-compose.yml 里 container_name),不走 compose:
# compose exec 会把 .env 里没设的可选变量逐个打 warning 到 stderr,几十行噪音淹没进度。
PG_CONTAINER="${PG_CONTAINER:-channelhub-postgres}"
psql_local() {
  docker exec -i "$PG_CONTAINER" psql -U "$PG_USER" -d "${PG_DB_OVERRIDE:-$PG_DB}" -v ON_ERROR_STOP=1 "$@"
}

# 可搬运的列:排除自增主键、生成列、snapshot_id
columns_of() {
  psql_local -Atc "
    SELECT string_agg(column_name, ', ' ORDER BY ordinal_position)
    FROM information_schema.columns
    WHERE table_schema = 'raw' AND table_name = '$1'
      AND is_identity = 'NO' AND is_generated = 'NEVER'
      AND column_name <> 'snapshot_id'"
}

cmd_export() {
  [[ -f .env ]] || { echo "✗ 未找到 .env" >&2; exit 1; }
  echo "-- ci_sync_data export $(date -u +%FT%TZ) from $(hostname)"
  echo "BEGIN;"
  for t in "${TABLES[@]}"; do
    cols="$(columns_of "$t")"
    [[ -n "$cols" ]] || { echo "✗ raw.$t 不存在或无可导出列" >&2; exit 1; }
    n="$(psql_local -Atc "SELECT count(*) FROM raw.$t")"
    echo "  · raw.$t: $n 行" >&2
    cat <<SQL
-- ---- raw.$t ($n 行) ----
CREATE TEMP TABLE stage_$t AS SELECT $cols FROM raw.$t WITH NO DATA;
COPY stage_$t ($cols) FROM STDIN WITH (FORMAT csv, HEADER true);
SQL
    psql_local -Atc "COPY (SELECT $cols FROM raw.$t) TO STDOUT WITH (FORMAT csv, HEADER true)"
    cat <<SQL
\\.
WITH ins AS (
  INSERT INTO raw.$t ($cols) SELECT $cols FROM stage_$t
  ON CONFLICT DO NOTHING RETURNING 1)
SELECT 'raw.$t' AS "table", (SELECT count(*) FROM stage_$t) AS received,
       count(*) AS inserted FROM ins;
DROP TABLE stage_$t;
SQL
  done
  echo "COMMIT;"
}

cmd_import() {
  local src="${1:--}"
  [[ -f .env ]] || { echo "✗ 未找到 .env" >&2; exit 1; }
  # 不加 -1:流里自带 BEGIN/COMMIT;任一表出错 ON_ERROR_STOP 会让整笔回滚
  if [[ "$src" == "-" ]]; then
    psql_local
  else
    psql_local < "$src"
  fi
  echo "✓ 导入完成(received=收到行数,inserted=真正新增;差额即目标端已有)" >&2
}

case "${1:-}" in
  export) cmd_export ;;
  import) cmd_import "${2:--}" ;;
  *) sed -n '2,16p' "$0" >&2; exit 2 ;;
esac

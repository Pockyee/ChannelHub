#!/usr/bin/env bash
# ============================================================================
# PLZ→Bundesland 参照装载器:db/seed/plz_bundesland.csv → core.plz_bundesland(幂等)
# ----------------------------------------------------------------------------
# CSV 即权威:编辑 CSV 后重跑即生效(CSV 没有的删除、其余 upsert)。
# 前置:已应用 db/migrations/008_geo_plz_bundesland.sql(建表/暂存/同步函数)。
# 数据源:GeoNames(download.geonames.org/export/zip/DE.zip),每 PLZ 取众数州。
#
# 用法(在仓库根目录):
#   bash db/seed/load_plz_bundesland.sh
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV="$SCRIPT_DIR/plz_bundesland.csv"
PGUSER="${POSTGRES_USER:-channelhub}"
PGDB="${POSTGRES_DB:-channelhub}"
PSQL=(docker compose exec -T postgres psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1)

[[ -f "$CSV" ]] || { echo "✗ 找不到 CSV: $CSV" >&2; exit 1; }

ROWS=$(tail -n +2 "$CSV" | grep -c '[^[:space:],]' || true)
echo "CSV: $CSV — 数据行 $ROWS"
[[ "$ROWS" -gt 0 ]] || { echo "✗ 参照 CSV 为空,已中止(空表会让 bundesland 全为 unknown)。" >&2; exit 2; }

# 1) 清空暂存 → 2) \copy 灌入(跳过表头)→ 3) 同步
"${PSQL[@]}" -c "TRUNCATE core.plz_bundesland_stage;"
"${PSQL[@]}" -c "\copy core.plz_bundesland_stage(plz,bundesland) FROM STDIN WITH (FORMAT csv, HEADER true)" < "$CSV"
echo "--- 同步结果(removed / upserted / total)---"
"${PSQL[@]}" -c "SELECT * FROM core.sync_plz_bundesland();"
echo "✓ 完成。mart.v_psi 的 bundesland 列即时生效。"

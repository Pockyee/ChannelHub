#!/usr/bin/env bash
# ============================================================================
# 门店陈列档位装载器:db/seed/{big,small}_display_plz.csv → core.store_display_plz(幂等)
# ----------------------------------------------------------------------------
# CSV 即权威:编辑 CSV 后重跑即生效(两张 CSV 都没有的 PLZ 从参照表删除,其余 upsert)。
# 每张 CSV 只有一列 plz(带表头),档位由文件名决定 —— 不在 CSV 里存 tier,避免手填出错。
# 两张 CSV 都没命中的门店 = Without Display(由 mart.v_psi 的 coalesce 兜底,不需要第三张表)。
#
# 前置:已应用 db/migrations/011_core_display_plz.sql(建表/暂存/同步函数)。
#
# 用法(在仓库根目录):
#   bash db/seed/load_display_plz.sh
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIG_CSV="$SCRIPT_DIR/big_display_plz.csv"
SMALL_CSV="$SCRIPT_DIR/small_display_plz.csv"
PGUSER="${POSTGRES_USER:-channelhub}"
PGDB="${POSTGRES_DB:-channelhub}"
PSQL=(docker compose exec -T postgres psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1)

for csv in "$BIG_CSV" "$SMALL_CSV"; do
  [[ -f "$csv" ]] || { echo "✗ 找不到 CSV: $csv" >&2; exit 1; }
done

# 只取 plz 列的非空数据行;tr -d '\r' 兜住 Excel/Windows 存出来的 CRLF(否则会灌进 "04416\r")
plz_rows() { tail -n +2 "$1" | tr -d '\r' | cut -d, -f1 | grep -E '[^[:space:]]' || true; }

BIG_N=$(plz_rows "$BIG_CSV"   | wc -l)
SMALL_N=$(plz_rows "$SMALL_CSV" | wc -l)
echo "CSV: big=$BIG_N 行 / small=$SMALL_N 行"

# 单张为空是正常状态(名单尚未收集);两张都空说明多半是误操作 —— 中止,免得清空参照表。
if (( BIG_N == 0 && SMALL_N == 0 )); then
  echo "✗ 两张档位 CSV 都为空,已中止(会清空 core.store_display_plz,所有门店变 Without Display)。" >&2
  exit 2
fi

# 1) 清空暂存 → 2) 两张 CSV 各打上档位标签灌入 → 3) 同步
"${PSQL[@]}" -c "TRUNCATE core.store_display_plz_stage;"
load_tier() {  # $1=csv  $2=档位
  local n; n=$(plz_rows "$1" | wc -l)
  (( n > 0 )) || { echo "  · 跳过 $(basename "$1")(空名单)"; return 0; }
  plz_rows "$1" | sed "s/[[:space:]]*$/,$2/" \
    | "${PSQL[@]}" -c "\copy core.store_display_plz_stage(plz,display_tier) FROM STDIN WITH (FORMAT csv)"
  echo "  · $(basename "$1") → $2($n)"
}
load_tier "$BIG_CSV"   "Big Display"
load_tier "$SMALL_CSV" "Small Display"

echo "--- 同步结果(removed / upserted / total)---"
"${PSQL[@]}" -c "SELECT * FROM core.sync_display_plz();"
echo "✓ 完成。mart.v_psi.display_tier 即时生效(看板无需重建)。"

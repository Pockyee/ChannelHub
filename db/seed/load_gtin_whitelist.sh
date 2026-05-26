#!/usr/bin/env bash
# ============================================================================
# GTIN 白名单装载器：db/seed/gtin_whitelist.csv → core.gtin_whitelist（幂等）
# ----------------------------------------------------------------------------
# CSV 即权威：编辑 CSV 后重跑本脚本即生效（CSV 没有的会被删除、其余 upsert）。
# 前置：已应用 db/migrations/005_core_gtin_whitelist.sql（建表/函数/卡点视图）。
#
# 用法（在仓库根目录）：
#   bash db/seed/load_gtin_whitelist.sh
#   bash db/seed/load_gtin_whitelist.sh --allow-empty   # 明确允许清空（会清空 core！）
#
# 安全：CSV 无数据行时**默认中止**——空白名单会把 core/BI 全部挡空。
# ============================================================================
set -euo pipefail

ALLOW_EMPTY=0
[[ "${1:-}" == "--allow-empty" ]] && ALLOW_EMPTY=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV="$SCRIPT_DIR/gtin_whitelist.csv"
PGUSER="${POSTGRES_USER:-channelhub}"
PGDB="${POSTGRES_DB:-channelhub}"
PSQL=(docker compose exec -T postgres psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1)

[[ -f "$CSV" ]] || { echo "✗ 找不到 CSV: $CSV" >&2; exit 1; }

# 数据行数（去掉表头，统计非空行）
ROWS=$(tail -n +2 "$CSV" | grep -c '[^[:space:],]' || true)
echo "CSV: $CSV — 数据行 $ROWS"
if [[ "$ROWS" -eq 0 && "$ALLOW_EMPTY" -eq 0 ]]; then
  echo "✗ 白名单为空。空白名单会把 core/BI 全部挡空，已中止。" >&2
  echo "  先在 $CSV 填入你的 GTIN（每行一个，可带 note），或确需清空时加 --allow-empty。" >&2
  exit 2
fi

# 1) 清空暂存
"${PSQL[@]}" -c "TRUNCATE core.gtin_whitelist_stage;"

# 2) CSV → 暂存（\copy 从 stdin 读，跳过表头；按列表顺序映射，与表头名无关）
"${PSQL[@]}" -c "\copy core.gtin_whitelist_stage(gtin_input,note) FROM STDIN WITH (FORMAT csv, HEADER true)" < "$CSV"

# 3) 同步进白名单（删/改增）并打印结果
echo "--- 同步结果（removed / upserted / total）---"
"${PSQL[@]}" -c "SELECT * FROM core.sync_gtin_whitelist();"

TOTAL=$("${PSQL[@]}" -tAc "SELECT count(*) FROM core.gtin_whitelist;")
echo "白名单当前共 $TOTAL 条 GTIN。"
if [[ "$TOTAL" -eq 0 ]]; then
  echo "⚠️  白名单为 0 条：core 视图与 BI 现在将为空，直至加入 GTIN 后重跑本脚本。" >&2
fi
echo "✓ 完成。core 视图实时生效（视图实现，无需重跑 ETL）。"
echo "  排查被挡的产品：SELECT * FROM core.v_gtin_unmatched ORDER BY raw_rows DESC;"

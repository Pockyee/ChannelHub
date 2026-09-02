#!/usr/bin/env bash
# ============================================================================
# 竞品情报产品主数据装载器(幂等)
#   db/seed/ci_product.csv        → core.ci_product
#   db/seed/ci_product_alias.csv  → core.ci_product_alias(只管 match_method='manual' 行)
# ----------------------------------------------------------------------------
# CSV 即权威:编辑后重跑即生效(CSV 没有的删除、其余 upsert)。
# alias 表里由 flow 自动写入的 regex/llm 行不受本脚本增删影响。
# 前置:已应用 db/migrations/012_ci_core.sql。
#
# 用法(在仓库根目录):
#   bash db/seed/load_ci_product.sh
#   bash db/seed/load_ci_product.sh --allow-empty   # 明确允许清空产品主数据
#
# 安全:产品 CSV 无数据行时**默认中止** —— 空主数据会让整条情报线无的放矢。
# ============================================================================
set -euo pipefail

ALLOW_EMPTY=0
[[ "${1:-}" == "--allow-empty" ]] && ALLOW_EMPTY=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV_PROD="$SCRIPT_DIR/ci_product.csv"
CSV_ALIAS="$SCRIPT_DIR/ci_product_alias.csv"
PGUSER="${POSTGRES_USER:-channelhub}"
PGDB="${POSTGRES_DB:-channelhub}"
PSQL=(docker compose exec -T postgres psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1)

[[ -f "$CSV_PROD"  ]] || { echo "✗ 找不到 CSV: $CSV_PROD"  >&2; exit 1; }
[[ -f "$CSV_ALIAS" ]] || { echo "✗ 找不到 CSV: $CSV_ALIAS" >&2; exit 1; }

ROWS=$(tail -n +2 "$CSV_PROD" | grep -c '[^[:space:],]' || true)
echo "CSV: $CSV_PROD — 数据行 $ROWS"
if [[ "$ROWS" -eq 0 && "$ALLOW_EMPTY" -eq 0 ]]; then
  echo "✗ 产品主数据为空，整条情报线将无的放矢，已中止。" >&2
  echo "  先在 $CSV_PROD 填入产品，或确需清空时加 --allow-empty。" >&2
  exit 2
fi

# --- 1) 产品主数据 -----------------------------------------------------------
"${PSQL[@]}" -c "TRUNCATE core.ci_product_stage;"
"${PSQL[@]}" -c "\copy core.ci_product_stage(product_id,brand,model,display_name,ean,is_own,active,brand_regex,match_regex,notes) FROM STDIN WITH (FORMAT csv, HEADER true)" < "$CSV_PROD"
echo "--- 产品同步(removed / upserted / total)---"
"${PSQL[@]}" -c "SELECT * FROM core.sync_ci_product();"

# --- 2) 各源标识 alias -------------------------------------------------------
"${PSQL[@]}" -c "TRUNCATE core.ci_product_alias_stage;"
"${PSQL[@]}" -c "\copy core.ci_product_alias_stage(product_id,source_code,external_id,alias_text) FROM STDIN WITH (FORMAT csv, HEADER true)" < "$CSV_ALIAS"
echo "--- alias 同步(removed / upserted / total)---"
"${PSQL[@]}" -c "SELECT * FROM core.sync_ci_product_alias();"

# --- 3) 缺口清单:哪些「产品 × 关键源」还没填标识 ------------------------------
# 只列**靠稳定 id 直接取数**的源。eBay 与 mydealz 走关键词搜索(结果再逐条复核型号)，
# 不需要 alias，列进来只会制造永远清不掉的假缺口。
echo "--- 待补的标识(product × source)---"
"${PSQL[@]}" -tAF' ' -c "
  SELECT p.product_id, s.source_code
  FROM core.ci_product p
  CROSS JOIN (VALUES ('amazon_de'),('idealo'),('geizhals')) AS s(source_code)
  WHERE p.active
    AND NOT EXISTS (SELECT 1 FROM core.ci_product_alias a
                    WHERE a.product_id = p.product_id AND a.source_code = s.source_code)
  ORDER BY 1, 2;"

MISSING=$("${PSQL[@]}" -tAc "
  SELECT count(*) FROM core.ci_product p
  CROSS JOIN (VALUES ('amazon_de'),('idealo'),('geizhals')) AS s(source_code)
  WHERE p.active
    AND NOT EXISTS (SELECT 1 FROM core.ci_product_alias a
                    WHERE a.product_id = p.product_id AND a.source_code = s.source_code);")
if [[ "$MISSING" -gt 0 ]]; then
  echo "⚠️  还有 $MISSING 个「产品 × 源」缺标识 —— 这些组合当天不会产生观测。" >&2
  echo "   在 $CSV_ALIAS 补 (product_id,source_code,external_id,alias_text) 后重跑本脚本。" >&2
  echo "   amazon_de 填 ASIN;idealo/geizhals 填商品页 URL 里的**完整 slug 段**" >&2
  echo "   (idealo 形如 209545339_-winbot-w3-omni-ecovacs;geizhals 形如" >&2
  echo "    ecovacs-winbot-w3-omni-fensterreinigungsroboter-a3725054" >&2
  echo "    —— 实测 geizhals 短号 aNNNNNNN 返回 403，必须用长 slug)" >&2
fi
echo "✓ 完成。"

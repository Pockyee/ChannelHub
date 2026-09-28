#!/usr/bin/env bash
# ============================================================================
# 广告层广告主覆盖表装载器(幂等)
#   db/seed/ci_ad_advertiser.csv → core.ci_ad_advertiser
#   db/seed/ci_ad_domain.csv     → core.ci_ad_domain(落地页域名 → 品牌,见 021)
# ----------------------------------------------------------------------------
# CSV 即权威:整表替换(TRUNCATE + COPY 在同一事务里,失败整笔回滚)。
# **允许为空** —— 与产品主数据不同,这张表是可选覆盖:某品牌在某源没有行时,
# flows/ci_ads.py 退回按品牌名检索。填 id 只是让结果更准(排除转售商)、更省。
#
# 列:source_code,advertiser_id,brand,advertiser_name,active,notes
#   source_code   meta_ads | google_ads
#   advertiser_id Meta = Facebook 主页数字 id;Google = AR 开头的广告主 id;
#                 '*' 配 active=false 表示该品牌在该源禁用按名检索
#   brand         必须与 core.ci_product.brand 完全一致(如 ECOVACS、HUTT)
#   active        true = 白名单(只按这些 id 精确取);false = 黑名单(按名检索时排除)
# ci_ad_domain.csv 列:domain,brand,active,notes
#   domain        裸域名小写(imoostore.com);ci-ads 按它去透明度中心取素材 id。
#                 用于走代投代理共用账户、不能按账户白名单的品牌(见 021 文件头)
# 改完重跑本脚本,下一次 ci-ads 会顺带删掉 raw.ci_ad 里不再符合配置的旧行。
# 不知道 id 时先跑发现命令,见 docs/COMPETITIVE_INTEL.md「广告层」:
#   docker compose exec prefect-worker python flows/ci_ads.py discover
#
# 前置:已应用 db/migrations/019_ci_ads.sql 与 021_ci_ad_domain.sql。
# 用法(在仓库根目录):bash db/seed/load_ci_ad_advertiser.sh
# ============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CSV="$SCRIPT_DIR/ci_ad_advertiser.csv"
DOMAIN_CSV="$SCRIPT_DIR/ci_ad_domain.csv"
PGUSER="${POSTGRES_USER:-channelhub}"
PGDB="${POSTGRES_DB:-channelhub}"
PSQL=(docker compose exec -T postgres psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1)

for f in "$CSV" "$DOMAIN_CSV"; do
  [[ -f "$f" ]] || { echo "✗ 找不到 CSV: $f" >&2; exit 1; }
  echo "CSV: $f — 数据行 $(tail -n +2 "$f" | grep -c '[^[:space:],]' || true)"
done

# 去空行 + 保证末行有换行,否则 \. 会被拼进最后一条记录
{
  echo "BEGIN;"
  echo "TRUNCATE core.ci_ad_advertiser;"
  echo "COPY core.ci_ad_advertiser (source_code,advertiser_id,brand,advertiser_name,active,notes) FROM STDIN WITH (FORMAT csv, HEADER true);"
  grep -v '^[[:space:]]*$' "$CSV" | sed -e '$a\'
  echo '\.'
  echo "TRUNCATE core.ci_ad_domain;"
  echo "COPY core.ci_ad_domain (domain,brand,active,notes) FROM STDIN WITH (FORMAT csv, HEADER true);"
  grep -v '^[[:space:]]*$' "$DOMAIN_CSV" | sed -e '$a\'
  echo '\.'
  echo "COMMIT;"
} | "${PSQL[@]}" -q

# brand 对不上产品主数据的行在看板上归不了属 —— 提示而不中止(可能是先填广告主后补产品)
"${PSQL[@]}" -tAF' ' -c "
  SELECT '⚠️  品牌不在 core.ci_product:', a.source_code, a.advertiser_id, a.brand
  FROM core.ci_ad_advertiser a
  WHERE NOT EXISTS (SELECT 1 FROM core.ci_product p WHERE p.brand = a.brand)
  UNION ALL
  SELECT '⚠️  品牌不在 core.ci_product:', 'domain', d.domain, d.brand
  FROM core.ci_ad_domain d
  WHERE NOT EXISTS (SELECT 1 FROM core.ci_product p WHERE p.brand = d.brand);" >&2
echo "✓ 完成:core.ci_ad_advertiser 现有 $("${PSQL[@]}" -tAc 'SELECT count(*) FROM core.ci_ad_advertiser') 行," \
     "core.ci_ad_domain 现有 $("${PSQL[@]}" -tAc 'SELECT count(*) FROM core.ci_ad_domain') 行"

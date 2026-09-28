-- ============================================================================
-- 020 — 竞品情报：广告展示次数按日摊分（Ad Impressions 图用）
-- ----------------------------------------------------------------------------
-- 依赖：019_ci_ads.sql（raw.ci_ad）、012_ci_core.sql（core.ci_product）
--
-- 产出：
--   · mart.v_ci_ad_daily   一条广告 × 投放期内每一天 一行，带当天摊到的展示次数估算
--
-- ---------------------------------------------------------------------------
-- 为什么是「估算」
-- ---------------------------------------------------------------------------
-- raw.ci_ad 是状态型 upsert（见 019 文件头）：Google 只给一条广告**整个生命周期**的
-- 展示次数区间（times_shown lower/upper），没有逐日明细。要看「每个时间段的展示量
-- 波动」，只能把每条广告的累计值**平均摊到它的首末展示日之间**，再按日/周/月加总。
-- 因此：
--   · 单条广告内部的起伏看不到（投放期内被拉平成一条直线）；
--     看板上的波动来自「哪些广告在投、各自多大、投多久」的叠加，量级与趋势可信，
--     具体某一天的数字不要当真
--   · 取区间中点作主值，impressions_low / impressions_high 给上下界备查
--   · 仍在投的广告 last_shown 会随每日刷新后移，最近几天的数字会被后续数据改写
--
-- 只收 exposure_kind='impressions'（Google）。Meta 的 eu_total_reach 是**去重人数**，
-- 摊到每天再加总没有意义（同一个人被多天重复计），不进这张视图。
--
-- 行数 = Σ 各广告投放天数（imoo 线 2026-09 实测约两万行），视图直接展开即可，不必物化。
--
-- 应用（幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/020_ci_ad_daily.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- !! 视图列顺序 !! CREATE OR REPLACE VIEW 不允许在中间插列,新列一律追加在末尾。
CREATE OR REPLACE VIEW mart.v_ci_ad_daily AS
WITH b AS (
    -- 同 v_ci_ad_detail:广告只有品牌没有型号，按品牌归线
    SELECT brand, bool_or(is_own) AS is_own, min(line) AS line
    FROM core.ci_product GROUP BY brand
), a AS (
    SELECT a.*,
           (a.last_shown - a.first_shown + 1)::numeric AS n_days,
           coalesce(a.exposure_lower, a.exposure_upper)::numeric AS lo,
           coalesce(a.exposure_upper, a.exposure_lower)::numeric AS hi
    FROM raw.ci_ad a
    WHERE a.exposure_kind = 'impressions'
      AND a.first_shown IS NOT NULL
      AND a.last_shown  >= a.first_shown
)
SELECT
    d::date                                               AS ad_day,
    CASE a.source_code WHEN 'meta_ads'   THEN 'Meta'
                       WHEN 'google_ads' THEN 'Google'
                       ELSE a.source_code END             AS platform,
    a.source_code,
    a.brand,
    coalesce(b.is_own, false)                             AS is_own,
    a.ad_id,
    a.advertiser_name,
    a.ad_format,
    round((a.lo + a.hi) / 2 / a.n_days, 2)                AS impressions_est,
    round(a.lo / a.n_days, 2)                             AS impressions_low,
    round(a.hi / a.n_days, 2)                             AS impressions_high,
    b.line
FROM a
LEFT JOIN b ON b.brand = a.brand
CROSS JOIN LATERAL generate_series(a.first_shown, a.last_shown, interval '1 day') AS d;

COMMENT ON VIEW mart.v_ci_ad_daily IS
  '广告展示次数按日摊分(估算):生命周期累计区间平均摊到首末展示日,只含 Google impressions;见 020 文件头';

GRANT USAGE  ON SCHEMA mart TO bi_readonly;
GRANT SELECT ON mart.v_ci_ad_daily TO bi_readonly;

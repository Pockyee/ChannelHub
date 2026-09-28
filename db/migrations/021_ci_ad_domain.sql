-- ============================================================================
-- 021 — 竞品情报：广告层按落地页域名归属（core.ci_ad_domain）
-- ----------------------------------------------------------------------------
-- 依赖：019_ci_ads.sql（raw.ci_ad / core.ci_ad_advertiser）
--
-- 产出：
--   · core.ci_ad_domain   落地页域名 → 品牌（CSV 即权威，见 db/seed/ci_ad_domain.csv）
--
-- ---------------------------------------------------------------------------
-- 为什么要按域名，而不是按广告主 id
-- ---------------------------------------------------------------------------
-- 有的品牌不自己开 Google 广告账户，而是走代投代理。实测（2026-09）imoo 在德国的
-- 广告 95% 挂在 BlueVision Interactive Limited 的**共用**账户 AR10942234166510485505
-- 下 —— 这个账户在德国有 6.7 万条素材、46 个类目，imoo 只占 93 条。按账户白名单会把
-- 别家的广告全算成 imoo 的；BigQuery 的 creative_stats 又没有落地页/域名字段。
--
-- 能区分的只有「广告点进去去哪个域名」，而这只有透明度中心网页的域名检索能查到。
-- flows/ci_ads.py 每轮先按这里登记的域名去透明度中心取当前的素材 id 清单，
-- 再只用这些 id 去 BigQuery 取展示数据（数字仍全部来自官方数据集）。
-- 透明度中心那一步走的是网页背后的非官方接口，可能随 Google 改版失效：
-- 失效时 flow 退回用 raw.ci_ad 里已有的素材 id 继续刷新，并告警 domain_lookup_failed。
--
-- 行的含义：active=true 生效；false 保留备注但暂不检索。
-- 一个品牌登记了域名后，Google 源就**不再按品牌名检索**（与广告主白名单同理），
-- 该品牌的 Google 广告 = 域名命中的素材 ∪ core.ci_ad_advertiser 白名单账户的素材。
--
-- 应用（幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/021_ci_ad_domain.sql
-- ============================================================================

CREATE TABLE IF NOT EXISTS core.ci_ad_domain (
    -- 裸域名，小写，不带协议/路径（imoostore.com，不是 https://www.imoostore.com/）
    domain  text PRIMARY KEY CHECK (domain = lower(domain) AND domain ~ '^[a-z0-9.-]+\.[a-z]{2,}$'),
    brand   text NOT NULL,          -- 必须等于 core.ci_product.brand,否则看板归不了属
    active  boolean NOT NULL DEFAULT true,
    notes   text
);
COMMENT ON TABLE core.ci_ad_domain IS
  '广告层落地页域名 → 品牌;由 db/seed/ci_ad_domain.csv 经 load_ci_ad_advertiser.sh 同步。'
  'ci-ads 按域名从透明度中心取素材 id,再去 BigQuery 取数;见 021 文件头';

GRANT USAGE  ON SCHEMA core TO bi_readonly;
GRANT SELECT ON core.ci_ad_domain TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON core.ci_ad_domain FROM bi_readonly;

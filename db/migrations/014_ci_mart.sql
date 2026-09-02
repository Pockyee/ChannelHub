-- ============================================================================
-- 014 — MART 层：竞品情报 BI 口径视图(Superset 取数)
-- ----------------------------------------------------------------------------
-- 依赖：012_ci_core.sql、013_ci_raw.sql
--
-- 产出：
--   · mart.v_ci_price_daily     每日 × 产品 × 源 的价格带(最低/中位/最高、商家数)
--   · mart.v_ci_demand_proxy    BSR 与评论数的一阶差分 —— 唯一的需求信号
--   · mart.v_ci_share_of_voice  每周 × 产品 × 源 的提及量与独立作者数
--   · mart.v_ci_media_coverage  媒体评测覆盖明细
--   · mart.v_ci_compare         【主视图】自家 vs 竞品逐日并排 + 相对自家价差
--
-- 口径说明：
--   · 自家 HUTT 与竞品 ECOVACS 走**完全相同的采集路径**，同在 core.ci_product 里
--     以 is_own 区分。本层不读 Shopify 订单(mart.v_hutt_shop_orders)——自家销售
--     数据不再进本服务器，对照关系全部收敛在情报线内部，口径因此天然可比。
--   · 一阶差分用 lag() 取**上一个有采样的日子**，另给 days_since_prev 供归一化；
--     采样有缺口时(抓取失败/被封)不要直接把 delta 当日增量，须先除以间隔天数。
--     此处沿用 mart.v_psi 的 weeks_since_prev 惯例。
--   · 金额一律 cents 存、EUR 出;total = 售价 + 运费，售价缺失则为空(不补 0)。
--
-- !! 视图列顺序 !!
--   CREATE OR REPLACE VIEW 不允许在列表中间插列，**新列一律追加在末尾**。
--
-- 应用(幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS)：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/014_ci_mart.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ---------------------------------------------------------------------------
-- 1) 价格带：每日 × 产品 × 源
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ci_price_daily AS
SELECT
    o.observed_on,
    o.product_id,
    p.brand,
    p.display_name,
    p.is_own,
    o.source_code,
    s.display_name                                        AS source_name,
    count(DISTINCT o.merchant_name)                       AS merchant_cnt,
    round(min(o.total_cents)    / 100.0, 2)               AS min_total_eur,
    round(max(o.total_cents)    / 100.0, 2)               AS max_total_eur,
    round((percentile_cont(0.5) WITHIN GROUP (ORDER BY o.total_cents))::numeric / 100.0, 2)
                                                          AS median_total_eur,
    round(min(o.price_cents)    / 100.0, 2)               AS min_price_eur
FROM raw.ci_offer o
JOIN core.ci_product p ON p.product_id = o.product_id
LEFT JOIN core.ci_source s ON s.source_code = o.source_code
WHERE o.total_cents IS NOT NULL
GROUP BY o.observed_on, o.product_id, p.brand, p.display_name, p.is_own,
         o.source_code, s.display_name;

COMMENT ON VIEW mart.v_ci_price_daily IS
  '每日×产品×源的价格带(到手价:售价+运费);自家与竞品同表，用 is_own 拆图例';

-- ---------------------------------------------------------------------------
-- 2) 需求代理：BSR 与评论数的一阶差分
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ci_demand_proxy AS
SELECT
    t.observed_on,
    t.product_id,
    p.brand,
    p.display_name,
    p.is_own,
    t.source_code,
    t.rating_avg,
    t.rating_count,
    t.review_count,
    t.bsr_category,
    t.bsr_rank,
    t.in_stock,
    (t.observed_on - lag(t.observed_on) OVER w)           AS days_since_prev,
    (t.review_count - lag(t.review_count) OVER w)         AS review_delta,
    -- BSR 越小越好，故取 lag - current：正数 = 排名上升(变好)
    (lag(t.bsr_rank) OVER w - t.bsr_rank)                 AS bsr_improvement,
    round(t.price_cents / 100.0, 2)                       AS price_eur
FROM raw.ci_listing_stat t
JOIN core.ci_product p ON p.product_id = t.product_id
WINDOW w AS (PARTITION BY t.product_id, t.source_code ORDER BY t.observed_on);

COMMENT ON VIEW mart.v_ci_demand_proxy IS
  '需求信号:review_count 一阶差分 + Amazon BSR 变化;采样有缺口时先除 days_since_prev 再比较';

-- ---------------------------------------------------------------------------
-- 3) 声量：每周 × 产品 × 源
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ci_share_of_voice AS
SELECT
    date_trunc('week', coalesce(m.published_at, m.ingested_at))::date AS mention_week,
    m.product_id,
    p.brand,
    p.display_name,
    p.is_own,
    m.source_code,
    s.layer                                               AS source_layer,
    count(*)                                              AS mention_cnt,
    count(DISTINCT m.author_hash)                         AS author_cnt,
    sum(coalesce((m.engagement ->> 'score')::numeric, 0)) AS engagement_score
FROM raw.ci_mention m
JOIN core.ci_product p ON p.product_id = m.product_id
LEFT JOIN core.ci_source s ON s.source_code = m.source_code
GROUP BY 1, m.product_id, p.brand, p.display_name, p.is_own, m.source_code, s.layer;

COMMENT ON VIEW mart.v_ci_share_of_voice IS
  '每周×产品×源的提及量/独立作者数;engagement_score 是各源自报热度的粗略归一，跨源比较仅供参考';

-- ---------------------------------------------------------------------------
-- 4) 媒体覆盖明细
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ci_media_coverage AS
SELECT
    m.published_at,
    m.published_at::date                                  AS published_on,
    m.product_id,
    p.brand,
    p.display_name,
    p.is_own,
    m.source_code,
    s.display_name                                        AS outlet,
    m.title,
    m.url,
    m.lang,
    length(coalesce(m.body, ''))                          AS body_len,
    -- 可点击标题：Superset 的 Table 图默认把单元格当纯文本，URL 不会变成链接。
    -- 这一列产出 <a>，配合图上的 allow_render_html=true 才能点开。
    -- !! 标题来自**抓取的外部网页**，直接当 HTML 渲染就是 XSS 入口 —— 必须在源头
    --    转义(& 要最先替换，否则会把后面替出来的实体再转一次)。Superset 的
    --    HTML_SANITIZATION 是第二道防线，不是免转义的理由。
    '<a href="' ||
      replace(replace(coalesce(m.url, ''), '&', '&amp;'), '"', '&quot;') ||
      '" target="_blank" rel="noopener noreferrer">' ||
      replace(replace(replace(replace(
        coalesce(nullif(m.title, ''), m.url, '(no title)'),
        '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), '"', '&quot;') ||
      '</a>'                                              AS title_link
FROM raw.ci_mention m
JOIN core.ci_product p  ON p.product_id  = m.product_id
JOIN core.ci_source  s  ON s.source_code = m.source_code AND s.layer = 'media';

COMMENT ON VIEW mart.v_ci_media_coverage IS
  '媒体评测覆盖明细(哪家媒体、什么时候、评了哪款);正文长度用于粗筛「顺带提一句」与「专门评测」';

-- ---------------------------------------------------------------------------
-- 5) 【主视图】自家 vs 竞品逐日并排
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ci_compare AS
WITH spine AS (            -- 有任意一类观测的 (日, 产品) 组合
    SELECT observed_on AS d, product_id FROM raw.ci_offer
    UNION
    SELECT observed_on,      product_id FROM raw.ci_listing_stat
    UNION
    SELECT coalesce(published_at, ingested_at)::date, product_id
      FROM raw.ci_mention WHERE product_id IS NOT NULL
),
price AS (                 -- 跨源最低到手价 = 市场最优价
    SELECT observed_on AS d, product_id,
           min(total_cents)                AS best_total_cents,
           count(DISTINCT merchant_name)   AS merchant_cnt
    FROM raw.ci_offer
    WHERE total_cents IS NOT NULL
    GROUP BY 1, 2
),
amz AS (
    SELECT observed_on AS d, product_id, bsr_rank, rating_avg, review_count
    FROM raw.ci_listing_stat
    WHERE source_code = 'amazon_de'
),
rev AS (                   -- 跨源评论数合计(各源累计值求和)
    SELECT observed_on AS d, product_id, sum(review_count) AS review_total
    FROM raw.ci_listing_stat
    WHERE review_count IS NOT NULL
    GROUP BY 1, 2
),
men AS (
    SELECT coalesce(published_at, ingested_at)::date AS d, product_id, count(*) AS cnt
    FROM raw.ci_mention WHERE product_id IS NOT NULL
    GROUP BY 1, 2
),
own_price AS (             -- 自家当日最优价，供竞品行算价差
    SELECT p.d, min(p.best_total_cents) AS own_best_cents
    FROM price p
    JOIN core.ci_product cp ON cp.product_id = p.product_id AND cp.is_own
    GROUP BY p.d
)
SELECT
    sp.d                                                  AS observed_on,
    sp.product_id,
    cp.brand,
    cp.display_name,
    cp.is_own,
    round(pr.best_total_cents / 100.0, 2)                 AS best_total_eur,
    pr.merchant_cnt,
    a.bsr_rank                                            AS amazon_bsr,
    a.rating_avg                                          AS amazon_rating,
    a.review_count                                        AS amazon_review_count,
    rv.review_total,
    -- 近 7 日提及量(含当日)；采样缺口不影响，按自然日窗口算
    sum(coalesce(mn.cnt, 0)) OVER (
        PARTITION BY sp.product_id ORDER BY sp.d
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
    )                                                     AS mentions_7d,
    -- 相对自家最优价的价差：正数 = 该款比我们贵。自家行恒为 0
    round((pr.best_total_cents - op.own_best_cents) / 100.0, 2)
                                                          AS price_gap_vs_own_eur
FROM spine sp
JOIN core.ci_product cp ON cp.product_id = sp.product_id
LEFT JOIN price     pr ON pr.d = sp.d AND pr.product_id = sp.product_id
LEFT JOIN amz       a  ON a.d  = sp.d AND a.product_id  = sp.product_id
LEFT JOIN rev       rv ON rv.d = sp.d AND rv.product_id = sp.product_id
LEFT JOIN men       mn ON mn.d = sp.d AND mn.product_id = sp.product_id
LEFT JOIN own_price op ON op.d = sp.d;

COMMENT ON VIEW mart.v_ci_compare IS
  '主视图:自家(is_own)与竞品逐日并排的价格/BSR/评分/评论/声量 + 相对自家最优价的价差;'
  '不依赖 Shopify 订单 —— 自家与竞品同源同口径采集，因此可直接比较';

-- ---------------------------------------------------------------------------
-- 只读授权
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA mart TO bi_readonly;
GRANT SELECT ON mart.v_ci_price_daily, mart.v_ci_demand_proxy,
                mart.v_ci_share_of_voice, mart.v_ci_media_coverage,
                mart.v_ci_compare TO bi_readonly;

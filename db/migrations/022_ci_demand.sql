-- ============================================================================
-- 022 — 竞品情报：需求层（Google Trends 搜索热度 + 配套 App 商店数据）
-- ----------------------------------------------------------------------------
-- 依赖：012_ci_core.sql（core.ci_source / core.ci_product）、019（layer 约束写法）
--
-- 产出：
--   · core.ci_source.layer 放行新层 'demand'，登记 google_trends / google_play / app_store
--   · core.ci_product.kind 放行 'listing'（Amazon 补充 listing 行，见下）
--   · core.ci_trend_term      每条情报线要比较的搜索词（CSV 即权威：db/seed/ci_trend_term.csv）
--   · core.ci_app             每个品牌的配套 App（CSV 即权威：db/seed/ci_app.csv）
--   · raw.ci_search_trend     Google Trends 周序列，按抓取日留全量快照
--   · raw.ci_app_stat         App 商店页日频指标（安装量 / 评分 / 评分数）
--   · mart.v_ci_search_trend / v_ci_app_daily  看板用
--
-- ---------------------------------------------------------------------------
-- 为什么要这一层
-- ---------------------------------------------------------------------------
-- 价格 / 提及 / 广告都是「可见度」。两个品牌都没有公开销量，报告里「谁卖得好」一直是推断。
-- 这一层补三个可回溯历史的需求 / 存量代理：
--   · 搜索热度：品牌词在德国的 Google 搜索量（相对值），5 年周序列，一次就能回溯
--   · App 存量：手表必须配 App 才能用，App 的安装量与评分数 ≈ 装机量
--     （App Store 的评分数按国家区分，德国区评分数是最直接的德国装机量代理）
--
-- 刻意**不采** App 评论：Google Play 的评论接口（/_/…batchexecute、/store/getreviews）
-- 与 Apple 的评论 RSS（/*/rss/*）都在各自 robots.txt 的 Disallow 里。robots 是本项目
-- 的硬约束（见 ci_common._robots_allows），所以只取 robots 允许的商店详情页与官方
-- iTunes lookup 接口，评论的月度节奏这条线索放弃。
-- Amazon BSR 走既有的价格层（raw.ci_listing_stat），不在本迁移。
--
-- ---------------------------------------------------------------------------
-- 口径（看数前必读）
-- ---------------------------------------------------------------------------
--   · Google Trends 的值是**同一次请求内**的相对值（该请求里最高的那一周 = 100）。
--     不同请求之间不可比，不同抓取日同一周的值也可能略有不同（Google 抽样）。
--     所以 raw 按 fetched_on 留全量快照，视图只取每条线**最新一次**抓取；
--     一条线的所有词必须在同一次请求里（≤ 5 个词），这由 flow 保证。
--   · Google Play 的安装量与评分数是**全球**的，不是德国的；德国维度只能看德语评论。
--     installs_exact 是商店页内嵌的精确安装数（页面只显示「100.000+」这种档位）。
--   · App Store 的评分数是**德国区**的（iTunes lookup 带 country=de）。
--
-- 应用（幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/022_ci_demand.sql
-- ============================================================================

-- 019 已把 layer 约束换成含 'ads' 的五层；这里再放行 'demand'（写法同 019，可重放）
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'core.ci_source'::regclass
      AND conname  = 'ci_source_layer_check'
      AND pg_get_constraintdef(oid) NOT LIKE '%''demand''%'
  ) THEN
    ALTER TABLE core.ci_source DROP CONSTRAINT ci_source_layer_check;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'core.ci_source'::regclass AND conname = 'ci_source_layer_check'
  ) THEN
    ALTER TABLE core.ci_source ADD CONSTRAINT ci_source_layer_check
      CHECK (layer IN ('price','retail','social','media','ads','demand'));
  END IF;
END $$;

-- core.ci_product.kind 放行 'listing'：**只按 ASIN 取 Amazon 数据**的补充 listing 行。
-- 同一型号在 Amazon 上可能有多组父商品(imoo 有 IMOO Direct 与另一组法语变体名的 listing)，
-- 或同一型号有运营商版(Telekom eSIM 版 X6 Play)。每组要单独一行才能各看各的排名与评分数
-- (raw.ci_listing_stat 一个产品一天只收一条 Amazon 观测)。listing 行不填正则，
-- load_products() 默认只要 model，所以提及 / 广告 / 型号消歧都看不见它，不会造成「同型号两行」的歧义。
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'core.ci_product'::regclass
      AND conname  = 'ck_ci_product_kind'
      AND pg_get_constraintdef(oid) NOT LIKE '%''listing''%'
  ) THEN
    ALTER TABLE core.ci_product DROP CONSTRAINT ck_ci_product_kind;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'core.ci_product'::regclass AND conname = 'ck_ci_product_kind'
  ) THEN
    ALTER TABLE core.ci_product ADD CONSTRAINT ck_ci_product_kind
      CHECK (kind IN ('model','series','listing'));
  END IF;
END $$;

-- ON CONFLICT 只更新 display_name/notes：active 是运维开关，重放不能把人工停用的源打开（见 012）
INSERT INTO core.ci_source (source_code, display_name, layer, access_mode, base_url, notes) VALUES
  ('google_trends', 'Google Trends', 'demand', 'scrape', 'https://trends.google.com',
   '网页背后的非官方接口 /trends/api/explore + widgetdata/multiline;值为同一请求内相对值(0–100)'),
  ('google_play',   'Google Play',   'demand', 'scrape', 'https://play.google.com',
   '只取商店详情页 /store/apps/details 内嵌 JSON(ds:5):安装量/评分;评论接口被 robots 禁止,不采'),
  ('app_store',     'Apple App Store', 'demand', 'api',  'https://itunes.apple.com',
   'iTunes lookup API(官方、免鉴权),country=de 取德国区评分数')
ON CONFLICT (source_code) DO UPDATE
  SET display_name = EXCLUDED.display_name, notes = EXCLUDED.notes;

-- ---------------------------------------------------------------------------
-- 配置表（CSV 即权威，由 db/seed/load_ci_product.sh 同步）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.ci_trend_term (
    line       text NOT NULL,                 -- 情报线（core.ci_product.line）
    term       text NOT NULL,                 -- 原样发给 Google Trends 的搜索词
    brand      text,                          -- 品牌词填 core.ci_product.brand;品类词留空
    sort_order int  NOT NULL DEFAULT 0,       -- 同线内的请求顺序(不影响数值)
    active     boolean NOT NULL DEFAULT true,
    notes      text,
    PRIMARY KEY (line, term)
);
COMMENT ON TABLE core.ci_trend_term IS
  '需求层:每条情报线在 Google Trends 上并排比较的搜索词(每线 ≤ 5 个 active);'
  '由 db/seed/ci_trend_term.csv 经 load_ci_product.sh 同步';

CREATE TABLE IF NOT EXISTS core.ci_app (
    store    text NOT NULL CHECK (store IN ('google_play','app_store')),
    app_id   text NOT NULL,                   -- Play 包名 / App Store 数字 id
    brand    text NOT NULL,                   -- 必须等于 core.ci_product.brand
    line     text NOT NULL,
    app_name text,
    active   boolean NOT NULL DEFAULT true,
    notes    text,
    PRIMARY KEY (store, app_id)
);
COMMENT ON TABLE core.ci_app IS
  '需求层:品牌的配套 App(装机量代理);由 db/seed/ci_app.csv 经 load_ci_product.sh 同步';

-- ---------------------------------------------------------------------------
-- 事实表（append-only，与其它 raw.ci_* 同一惯例：不建外键、日频幂等键是实体列）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ci_search_trend (
    trend_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fetched_on       date NOT NULL,           -- 抓取日：同一周在不同抓取日的值可能不同
    line             text NOT NULL,
    geo              text NOT NULL,           -- 'DE'
    timeframe        text NOT NULL,           -- 'today 5-y'
    term             text NOT NULL,
    period_start     date NOT NULL,           -- 周起始日(Google 按周日开始)
    value            smallint NOT NULL CHECK (value BETWEEN 0 AND 100),
    has_data         boolean,                 -- Google 标的「该周有足够数据」
    is_partial       boolean NOT NULL DEFAULT false,   -- 当周尚未结束
    ingestion_run_id text,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_ci_search_trend UNIQUE (line, geo, timeframe, term, period_start, fetched_on)
);
CREATE INDEX IF NOT EXISTS ix_cisearchtrend_latest ON raw.ci_search_trend (line, geo, timeframe, fetched_on);

CREATE TABLE IF NOT EXISTS raw.ci_app_stat (
    app_stat_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store            text NOT NULL,
    app_id           text NOT NULL,
    observed_on      date NOT NULL,
    installs_text    text,                    -- 页面显示的档位，如 '1.000.000+'
    installs_min     bigint,                  -- 档位下限
    installs_exact   bigint,                  -- Play 内嵌的精确数(可能缺)
    rating_avg       numeric(4,3),
    rating_count     bigint,
    review_count     bigint,                  -- 写了文字的评论数(Play 才有)
    app_version      text,
    app_updated_on   date,
    ingestion_run_id text,
    observed_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_ci_app_stat UNIQUE (store, app_id, observed_on)
);

-- ---------------------------------------------------------------------------
-- 看板视图（新列一律追加在末尾，见 014 文件头）
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_ci_search_trend AS
WITH latest AS (
    SELECT line, geo, timeframe, max(fetched_on) AS fetched_on
    FROM raw.ci_search_trend
    GROUP BY 1, 2, 3
)
SELECT
    t.period_start                          AS trend_week,
    t.term,
    coalesce(tt.brand, 'Category')          AS brand,
    coalesce(p.is_own, false)               AS is_own,
    t.value,
    t.is_partial,
    t.geo,
    t.fetched_on,
    t.line
FROM raw.ci_search_trend t
JOIN latest l
  ON l.line = t.line AND l.geo = t.geo AND l.timeframe = t.timeframe AND l.fetched_on = t.fetched_on
LEFT JOIN core.ci_trend_term tt ON tt.line = t.line AND tt.term = t.term
LEFT JOIN (SELECT brand, bool_or(is_own) AS is_own FROM core.ci_product GROUP BY brand) p
  ON p.brand = tt.brand;
COMMENT ON VIEW mart.v_ci_search_trend IS
  'Google Trends 周序列,每条线只取最新一次抓取(值只在同一次请求内可比,见 022 文件头)';

CREATE OR REPLACE VIEW mart.v_ci_app_daily AS
SELECT
    s.observed_on,
    a.brand,
    a.app_name,
    s.store,
    a.brand || ' · ' || CASE s.store WHEN 'google_play' THEN 'Google Play' ELSE 'App Store DE' END
                                            AS app_label,
    s.installs_text,
    s.installs_min,
    s.installs_exact,
    s.rating_avg,
    s.rating_count,
    s.review_count,
    (s.observed_on - lag(s.observed_on) OVER w)        AS days_since_prev,
    (s.rating_count - lag(s.rating_count) OVER w)      AS rating_count_delta,
    (s.installs_exact - lag(s.installs_exact) OVER w)  AS installs_delta,
    coalesce(p.is_own, false)               AS is_own,
    a.line
FROM raw.ci_app_stat s
JOIN core.ci_app a ON a.store = s.store AND a.app_id = s.app_id
LEFT JOIN (SELECT brand, bool_or(is_own) AS is_own FROM core.ci_product GROUP BY brand) p
  ON p.brand = a.brand
WINDOW w AS (PARTITION BY s.store, s.app_id ORDER BY s.observed_on);
COMMENT ON VIEW mart.v_ci_app_daily IS
  'App 商店页日频指标;Play 的安装量/评分数是全球口径,App Store 是德国区;采样有缺口时 delta 先除 days_since_prev';

-- ---------------------------------------------------------------------------
-- 只读角色
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA core, raw, mart TO bi_readonly;
GRANT SELECT ON core.ci_trend_term, core.ci_app,
                raw.ci_search_trend, raw.ci_app_stat,
                mart.v_ci_search_trend, mart.v_ci_app_daily
  TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON core.ci_trend_term, core.ci_app, raw.ci_search_trend, raw.ci_app_stat
  FROM bi_readonly;

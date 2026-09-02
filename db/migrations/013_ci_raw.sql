-- ============================================================================
-- 013 — RAW 层：竞品情报采集落地(快照 / 报价 / 商品页指标 / 提及 / 待审)
-- ----------------------------------------------------------------------------
-- 依赖：012_ci_core.sql(core.ci_product / ci_product_alias / ci_source)
--
-- 产出：
--   · raw.ci_snapshot      每次抓取的原样存档指针(MinIO ci-archive 的对象键)
--   · raw.ci_offer         价格观测，append-only，一天一源一商家一行
--   · raw.ci_listing_stat  商品页日频指标(评分/评论数/BSR/在售)，一天一源一品一行
--   · raw.ci_mention       统一提及事实(reddit/youtube/mydealz 评论/媒体文章)
--   · raw.ci_unmatched     型号匹配不上的待审队列
--
-- 设计要点：
--   · **不建到 core.ci_product 的外键**：raw 是 append-only 历史留痕，产品从 seed
--     CSV 下架不该牵动已采集的观测(见 012 文件头)。
--   · **observed_on date 单列**而非 observed_at::date 表达式：UNIQUE 约束不能用
--     表达式，日频幂等键必须是实体列。重跑当天的 flow → ON CONFLICT 命中，不产生
--     重复观测；这是「一天一爬」能安全重试的基础。
--   · **绝不原地 update 价格**：价格历史曲线是本项目主要价值，覆盖即销毁。
--     ci_mention 的 engagement 是唯一例外(播放量/点赞会长)，单独用
--     engagement_updated_at 记录刷新时点，正文本身仍不可变。
--   · content_hash 去重：页面没变就不再存快照、不再送 LLM 富化。
--
-- 应用(幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS)：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/013_ci_raw.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- 抓取快照指针：正文原样存 MinIO(桶 ci-archive)，库里只留元数据
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ci_snapshot (
    snapshot_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code      text NOT NULL,
    product_id       text,                  -- 与产品无关的列表页可为空
    url              text NOT NULL,
    http_status      integer,
    content_hash     text NOT NULL,         -- sha256(正文)，去重键
    object_key       text,                  -- ci/<source>/<yyyy-mm-dd>/<hash>.<ext>
    content_type     text,
    byte_size        integer,
    fetched_at       timestamptz NOT NULL DEFAULT now(),
    ingestion_run_id text,
    CONSTRAINT uq_ci_snapshot UNIQUE (source_code, url, content_hash)
);
COMMENT ON TABLE raw.ci_snapshot IS
  '抓取原样存档指针;同一 URL 内容未变则 content_hash 命中不重复存 —— 解析器改版后可回溯重跑';

CREATE INDEX IF NOT EXISTS ix_cisnap_source_fetched ON raw.ci_snapshot (source_code, fetched_at);
CREATE INDEX IF NOT EXISTS ix_cisnap_product        ON raw.ci_snapshot (product_id);

-- ---------------------------------------------------------------------------
-- 价格观测(append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ci_offer (
    offer_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id     text NOT NULL,
    source_code    text NOT NULL,
    merchant_name  text NOT NULL,           -- 比价站上的具体商家;直营站填站名本身
    price_cents    integer,
    currency       text NOT NULL DEFAULT 'EUR',
    shipping_cents integer,
    -- 到手价:价格缺失时为空而非 0(0 会被当成"免费"污染 min() 聚合)
    total_cents integer GENERATED ALWAYS AS (
        CASE WHEN price_cents IS NULL THEN NULL
             ELSE price_cents + coalesce(shipping_cents, 0) END
    ) STORED,
    availability   text,
    offer_url      text,
    observed_on    date NOT NULL,           -- 采样日 = 日频幂等键
    observed_at    timestamptz NOT NULL DEFAULT now(),
    snapshot_id    bigint,
    ingestion_run_id text,
    CONSTRAINT uq_ci_offer UNIQUE (source_code, product_id, merchant_name, observed_on)
);
COMMENT ON TABLE raw.ci_offer IS
  '竞品/自家价格观测(append-only，一天一源一商家一行);绝不原地 update —— 价格历史即本项目主要价值';

CREATE INDEX IF NOT EXISTS ix_cioffer_product_day ON raw.ci_offer (product_id, observed_on);
CREATE INDEX IF NOT EXISTS ix_cioffer_source_day  ON raw.ci_offer (source_code, observed_on);

-- ---------------------------------------------------------------------------
-- 商品页日频指标：评分/评论数/BSR/在售 —— 一阶差分即需求信号
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ci_listing_stat (
    stat_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id     text NOT NULL,
    source_code    text NOT NULL,
    rating_avg     numeric(3,2),            -- 0.00–5.00
    rating_count   integer,
    review_count   integer,                 -- 一阶差分 = 销量速度代理
    bsr_category   text,                    -- Amazon「Bestseller-Rang」所属类目
    bsr_rank       integer,                 -- 排名，越小越好
    price_cents    integer,                 -- 该站当时售价(冗余，便于单站看价)
    in_stock       boolean,
    observed_on    date NOT NULL,
    observed_at    timestamptz NOT NULL DEFAULT now(),
    snapshot_id    bigint,
    ingestion_run_id text,
    CONSTRAINT uq_ci_listing_stat UNIQUE (source_code, product_id, observed_on)
);
COMMENT ON TABLE raw.ci_listing_stat IS
  '零售商品页日频指标;bsr_rank(Amazon) 与 review_count 的一阶差分是全项目唯一的需求信号';
COMMENT ON COLUMN raw.ci_listing_stat.review_count IS
  '累计评论数;日增量 = 销量速度代理。必须日频采样，历史补不回来';

CREATE INDEX IF NOT EXISTS ix_cistat_product_day ON raw.ci_listing_stat (product_id, observed_on);

-- ---------------------------------------------------------------------------
-- 统一提及事实：reddit / youtube / mydealz 评论 / amazon 评论 / 媒体文章
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ci_mention (
    mention_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id   text,                      -- 匹配不上时为空并进 ci_unmatched
    source_code  text NOT NULL,
    external_id  text NOT NULL,             -- 该源内的稳定 id(评论 id/视频 id/文章 URL)
    url          text,
    title        text,
    body         text,
    lang         text,
    -- GDPR：只存加盐哈希，不落库显示名。正文按正当利益保留，但作者身份不可检索
    author_hash  text,
    published_at timestamptz,
    engagement   jsonb NOT NULL DEFAULT '{}'::jsonb,   -- {views,likes,comments,score,…}
    engagement_updated_at timestamptz,
    snapshot_id  bigint,
    ingestion_run_id text,
    ingested_at  timestamptz NOT NULL DEFAULT now(),
    -- 唯一键含 product_id:一篇对比评测会提及多款 —— 那正是媒体层最有价值的内容，
    -- 不能被迫二选一。NULLS NOT DISTINCT(PG15+) 保证「匹配不上」的行只留一条。
    CONSTRAINT uq_ci_mention UNIQUE NULLS NOT DISTINCT (source_code, external_id, product_id)
);

-- 前向修正:早期版本唯一键不含 product_id。仅在列数不符时重建，避免每次 deploy 重建索引。
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    WHERE c.conrelid = 'raw.ci_mention'::regclass
      AND c.conname  = 'uq_ci_mention'
      AND array_length(c.conkey, 1) = 3
  ) THEN
    ALTER TABLE raw.ci_mention DROP CONSTRAINT IF EXISTS uq_ci_mention;
    ALTER TABLE raw.ci_mention ADD CONSTRAINT uq_ci_mention
      UNIQUE NULLS NOT DISTINCT (source_code, external_id, product_id);
  END IF;
END $$;
COMMENT ON TABLE raw.ci_mention IS
  '各源用户/媒体提及统一事实表;正文不可变，仅 engagement 允许刷新(播放量会长)。'
  '一篇文档提及 N 款 = N 行，共享同一 external_id';
COMMENT ON COLUMN raw.ci_mention.author_hash IS
  'GDPR:作者身份只存 CI_AUTHOR_SALT 加盐哈希，绝不落库显示名';

CREATE INDEX IF NOT EXISTS ix_cimention_product_pub ON raw.ci_mention (product_id, published_at);
CREATE INDEX IF NOT EXISTS ix_cimention_source      ON raw.ci_mention (source_code, ingested_at);

-- ---------------------------------------------------------------------------
-- 型号匹配失败的待审队列(配合 raw.ingest_alert 告警去重)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ci_unmatched (
    unmatched_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code   text NOT NULL,
    external_id   text NOT NULL,
    raw_title     text,
    url           text,
    seen_count    integer NOT NULL DEFAULT 1,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz NOT NULL DEFAULT now(),
    resolved      boolean NOT NULL DEFAULT false,
    CONSTRAINT uq_ci_unmatched UNIQUE (source_code, external_id)
);
COMMENT ON TABLE raw.ci_unmatched IS
  '型号消歧失败的待审队列;人工确认后补进 db/seed/ci_product_alias.csv 并重跑 loader';

-- ---------------------------------------------------------------------------
-- 只读授权
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA raw TO bi_readonly;
GRANT SELECT ON raw.ci_snapshot, raw.ci_offer, raw.ci_listing_stat,
                raw.ci_mention, raw.ci_unmatched TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON raw.ci_snapshot, raw.ci_offer, raw.ci_listing_stat,
     raw.ci_mention, raw.ci_unmatched FROM bi_readonly;

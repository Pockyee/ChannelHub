-- ============================================================================
-- 019 — 竞品情报：广告层（Meta Ad Library + Google Ads Transparency Center）
-- ----------------------------------------------------------------------------
-- 依赖：012_ci_core.sql（core.ci_source / core.ci_product）
--
-- 产出：
--   · core.ci_source.layer 放行新层 'ads'，并登记 meta_ads / google_ads 两个源
--   · core.ci_ad_advertiser   广告主覆盖表（可选；CSV 即权威，见 db/seed/ci_ad_advertiser.csv）
--   · raw.ci_ad               广告事实表（一条广告一行，状态型 upsert，见下）
--   · mart.v_ci_ad_detail     看板用明细 + 周粒度（Ads per Week 柱状图与 Ads 表共用）
--
-- ---------------------------------------------------------------------------
-- 为什么 raw.ci_ad 是「状态型 upsert」而不是 append-only
-- ---------------------------------------------------------------------------
-- 其它 raw.ci_* 表是日频观测（今天的价格 ≠ 昨天的价格，历史即价值），所以绝不原地改。
-- 广告不同：两个源给的都是**这条广告的生命周期累计值**——首末展示日、欧盟累计覆盖、
-- 累计展示次数区间。今天取到的值严格包含昨天的，存每日快照只会得到一串单调递增的
-- 重复行，看板要的「谁在投、投了多久、覆盖多大」一行就够。
-- 因此唯一键 (source_code, ad_id) 上 DO UPDATE 刷新可变列，只有 first_seen_at 不动。
--
-- ---------------------------------------------------------------------------
-- exposure 列为什么不叫 reach
-- ---------------------------------------------------------------------------
-- Meta 给的是 eu_total_reach —— **人数**；Google 给的是 times_shown 区间 —— **展示次数**。
-- 两者量纲不同，同一列叫 reach 会诱导把它们加总或直接比大小。exposure_kind 标明是哪种，
-- 视图里拼成带单位的文本，看板上不会被误读。
--
-- 应用（幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/019_ci_ads.sql
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1) 源注册：放行 layer='ads'
-- ---------------------------------------------------------------------------
-- 012 建表时 CHECK 只列了四层。012 每次 deploy 重放但 CREATE TABLE IF NOT EXISTS
-- 不会重建约束，所以这里换约束是安全的；全新库上 012 先建旧约束、019 再换新的。
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'core.ci_source'::regclass
      AND conname  = 'ci_source_layer_check'
      AND pg_get_constraintdef(oid) NOT LIKE '%''ads''%'
  ) THEN
    ALTER TABLE core.ci_source DROP CONSTRAINT ci_source_layer_check;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'core.ci_source'::regclass AND conname = 'ci_source_layer_check'
  ) THEN
    ALTER TABLE core.ci_source ADD CONSTRAINT ci_source_layer_check
      CHECK (layer IN ('price','retail','social','media','ads'));
  END IF;
END $$;

-- ON CONFLICT 只更新 display_name/notes：active 是运维开关，重放不能把人工停用的源打开（见 012）
INSERT INTO core.ci_source (source_code, display_name, layer, access_mode, base_url, notes) VALUES
  ('meta_ads',   'Meta Ad Library',               'ads', 'api', 'https://graph.facebook.com',
   'Graph API /ads_archive;欧盟触达过的广告不限类型均可查(DSA),带 eu_total_reach 与定向'),
  ('google_ads', 'Google Ads Transparency Center', 'ads', 'api', 'https://bigquery.googleapis.com',
   'BigQuery 公共数据集 google_ads_transparency_center.creative_stats(仅 EEA);无广告文案')
ON CONFLICT (source_code) DO UPDATE
  SET display_name = EXCLUDED.display_name, notes = EXCLUDED.notes;

-- ---------------------------------------------------------------------------
-- 2) 广告主覆盖表（可选）
-- ---------------------------------------------------------------------------
-- 空表也能跑：flow 退回按品牌名检索（Meta 用 search_terms + 主页名过品牌正则；
-- Google 用 advertiser_disclosed_name 过品牌正则）。行的含义：
--   active=true        白名单：该品牌在该源改为只按这些 id 精确取
--   active=false       黑名单：按名检索时排除这个 id（同名公司）
--   id='*' + false     禁用按名检索（品牌名太常见时；实测 HUTT 在 Google 上按名
--                      搜到 14 个广告主，全是画廊/牙科/T 恤店）
-- 配置收窄后 flow 会删掉 raw.ci_ad 里不再符合的旧行（见 flows/ci_ads.py:_prune）。
CREATE TABLE IF NOT EXISTS core.ci_ad_advertiser (
    source_code     text NOT NULL CHECK (source_code IN ('meta_ads','google_ads')),
    advertiser_id   text NOT NULL,          -- Meta: Facebook 主页数字 id;Google: AR 开头的广告主 id
    brand           text NOT NULL,          -- 必须等于 core.ci_product.brand,否则看板归不了属
    advertiser_name text,                   -- 仅备注用;真实名称以源返回为准
    active          boolean NOT NULL DEFAULT true,
    notes           text,
    -- 主键含 brand:'*'(禁用按名检索)是按品牌的开关,同一个源上可以有多个品牌各自禁用
    PRIMARY KEY (source_code, brand, advertiser_id)
);
-- 旧库的主键是 (source_code, advertiser_id) —— 每个源只能登记一行 '*',第二个品牌
-- (2026-09-28 的 TCL,HUTT 已占了 google_ads/*)装载直接撞主键。换成含 brand 的主键。
DO $$
BEGIN
  IF (SELECT array_length(conkey, 1) FROM pg_constraint
      WHERE conrelid = 'core.ci_ad_advertiser'::regclass AND contype = 'p') = 2 THEN
    ALTER TABLE core.ci_ad_advertiser DROP CONSTRAINT ci_ad_advertiser_pkey;
    ALTER TABLE core.ci_ad_advertiser ADD PRIMARY KEY (source_code, brand, advertiser_id);
  END IF;
END $$;
COMMENT ON TABLE core.ci_ad_advertiser IS
  '广告层广告主覆盖(可选);由 db/seed/ci_ad_advertiser.csv 经 load_ci_ad_advertiser.sh 同步。'
  'active=true 白名单(只按 id 取) / false 黑名单(按名检索时排除) / id=* 且 false 禁用按名检索';

-- ---------------------------------------------------------------------------
-- 3) 广告事实表
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.ci_ad (
    ad_row_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_code     text NOT NULL,              -- meta_ads | google_ads
    ad_id           text NOT NULL,              -- Meta Library id / Google creative id
    advertiser_id   text NOT NULL,
    advertiser_name text,
    brand           text,                       -- core.ci_product.brand 归属
    ad_format       text,                       -- Google: TEXT/IMAGE/VIDEO;Meta 无此字段
    surfaces        text[] NOT NULL DEFAULT '{}',   -- Meta: facebook/instagram/…;Google: YOUTUBE/SEARCH/…
    title           text,
    body            text,                       -- Google 数据集不含文案,恒为空
    languages       text[] NOT NULL DEFAULT '{}',
    first_shown     date,
    last_shown      date,
    is_active       boolean,
    exposure_lower  bigint,
    exposure_upper  bigint,
    exposure_kind   text CHECK (exposure_kind IN ('reach','impressions')),
    targeting       jsonb NOT NULL DEFAULT '{}'::jsonb,
    product_ids     text[] NOT NULL DEFAULT '{}',   -- 文案里命中的具体型号(文档语境,可多款)
    ad_url          text,                       -- 公开可访问的广告详情页;**绝不存带 token 的 URL**
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    last_seen_at    timestamptz NOT NULL DEFAULT now(),
    ingestion_run_id text,
    CONSTRAINT uq_ci_ad UNIQUE (source_code, ad_id)
);
COMMENT ON TABLE raw.ci_ad IS
  '竞品/自家广告(Meta Ad Library + Google Ads Transparency);状态型 upsert —— '
  '源给的是生命周期累计值,见 019 文件头';
COMMENT ON COLUMN raw.ci_ad.ad_url IS
  'Meta 的 ad_snapshot_url 内嵌 access_token,绝不入库;这里存的是 facebook.com/ads/library/?id= 公开页';

CREATE INDEX IF NOT EXISTS ix_ciad_brand_first ON raw.ci_ad (brand, first_shown);
CREATE INDEX IF NOT EXISTS ix_ciad_source_last ON raw.ci_ad (source_code, last_shown);

-- ---------------------------------------------------------------------------
-- 4) 看板视图
-- ---------------------------------------------------------------------------
-- !! 视图列顺序 !! CREATE OR REPLACE VIEW 不允许在中间插列,新列一律追加在末尾。
CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.v_ci_ad_detail AS
WITH b AS (
    -- 广告只有品牌没有型号，故按品牌归线;一个品牌只属于一条线(见 012 line 注释)
    SELECT brand, bool_or(is_own) AS is_own, min(line) AS line
    FROM core.ci_product GROUP BY brand
)
SELECT
    a.first_shown,
    a.last_shown,
    -- 按「开投那周」归周:柱状图回答的是「这周新上了多少条广告」
    date_trunc('week', coalesce(a.first_shown, a.first_seen_at::date))::date AS ad_week,
    CASE a.source_code WHEN 'meta_ads'   THEN 'Meta'
                       WHEN 'google_ads' THEN 'Google'
                       ELSE a.source_code END             AS platform,
    a.source_code,
    a.brand,
    coalesce(b.is_own, false)                             AS is_own,
    a.advertiser_name,
    a.ad_format,
    array_to_string(a.surfaces, ', ')                     AS surfaces,
    CASE WHEN a.is_active THEN 'running' ELSE 'ended' END AS status,
    CASE WHEN a.first_shown IS NOT NULL AND a.last_shown IS NOT NULL
         THEN a.last_shown - a.first_shown + 1 END        AS days_running,
    a.exposure_lower,
    a.exposure_upper,
    a.exposure_kind,
    -- 带单位的文本:reach(人数)与 impressions(次数)不可比,别让看板把它们当同一个数
    CASE
      WHEN a.exposure_lower IS NULL AND a.exposure_upper IS NULL THEN NULL
      WHEN a.exposure_upper IS NULL OR a.exposure_lower = a.exposure_upper
        THEN to_char(coalesce(a.exposure_lower, a.exposure_upper), 'FM999,999,999,999')
             || ' ' || coalesce(a.exposure_kind, '')
      ELSE to_char(coalesce(a.exposure_lower, 0), 'FM999,999,999,999') || '–'
           || to_char(a.exposure_upper, 'FM999,999,999,999')
           || ' ' || coalesce(a.exposure_kind, '')
    END                                                   AS exposure,
    a.title,
    left(a.body, 300)                                     AS body_snippet,
    array_to_string(a.product_ids, ', ')                  AS products,
    a.targeting,
    -- 可点击链接:同 v_ci_mention_detail,标题来自外部广告文案,必须在源头转义(& 最先替换)
    '<a href="' ||
      replace(replace(coalesce(a.ad_url, ''), '&', '&amp;'), '"', '&quot;') ||
      '" target="_blank" rel="noopener noreferrer">' ||
      replace(replace(replace(replace(
        coalesce(nullif(a.title, ''), nullif(left(a.body, 120), ''),
                 coalesce(a.advertiser_name, a.brand, 'Ad') || ' · ' || a.ad_id),
        '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), '"', '&quot;') ||
      '</a>'                                              AS ad_link,
    a.ad_id,
    a.first_seen_at,
    a.last_seen_at,
    b.line
FROM raw.ci_ad a
LEFT JOIN b ON b.brand = a.brand;

COMMENT ON VIEW mart.v_ci_ad_detail IS
  '广告明细(Meta + Google);ad_week=开投周,供 Ads per Week 柱状图;ad_link 需 allow_render_html';

-- ---------------------------------------------------------------------------
-- 5) 只读授权
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA core, mart TO bi_readonly;
GRANT SELECT ON core.ci_ad_advertiser, mart.v_ci_ad_detail TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON core.ci_ad_advertiser FROM bi_readonly;

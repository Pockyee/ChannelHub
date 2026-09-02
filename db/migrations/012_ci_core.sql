-- ============================================================================
-- 012 — CORE 层：竞品情报(Competitive Intelligence)的产品主数据与源注册表
-- ----------------------------------------------------------------------------
-- 背景：平台第二条数据线。对自家 HUTT 与竞品 ECOVACS WINBOT 三款擦窗机做日频
--       情报采集(价格 / 零售口碑 / 用户讨论 / 媒体评测)，最终在 Superset 出
--       品类 Market Intelligence 看板，并与自家真实需求(mart.v_hutt_shop_orders)
--       对照。命名沿用 raw/core/mart 分层 + 域前缀 ci_。
--
-- 产出：
--   · core.ci_product        产品主数据(自家 + 竞品同表，is_own 区分)，CSV 即权威
--   · core.ci_product_stage  CSV 装载暂存
--   · core.sync_ci_product() stage → 主数据(删/改增)
--   · core.ci_product_alias  各源标识(ASIN/idealo id/…) → product_id，CSV 即权威
--   · core.ci_product_alias_stage / core.sync_ci_product_alias()
--   · core.ci_source         源注册表(本迁移内 seed，与 flow 代码里的 source_code 耦合)
--
-- 设计要点：
--   · product_id 用人类可读短码做主键(如 'ecovacs-w3-omni')而非代理键 —— seed CSV
--     与 alias CSV 都靠它互相引用，人工维护时可读性 > 紧凑性。
--   · raw.ci_* 各表引用 product_id 但**不建外键**：raw 层是 append-only 历史留痕，
--     产品从 CSV 下架不应牵动(或阻塞删除)已采集的历史观测。
--   · match_regex 是型号消歧的唯一事实源。W2 OMNI / W2S OMNI / W2 PRO OMNI 互为
--     前缀，朴素包含匹配必然误配，必须靠带词边界的正则 + 长型号优先。
--     mart.v_ci_vs_own 里给 Shopify 订单打自家标记也复用同一列。
--
-- 装载：db/seed/load_ci_product.sh(一次读 ci_product.csv + ci_product_alias.csv)
--
-- 应用(幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS)：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/012_ci_core.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

-- ---------------------------------------------------------------------------
-- 产品主数据(CSV 即权威)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.ci_product (
    product_id   text PRIMARY KEY,          -- 人类可读短码，如 'ecovacs-w3-omni'
    brand        text NOT NULL,
    model        text NOT NULL,             -- 'WINBOT W3 OMNI'
    display_name text NOT NULL,             -- 看板显示名
    ean          text,                      -- 有则优先用它匹配，最可靠
    is_own       boolean NOT NULL DEFAULT false,
    active       boolean NOT NULL DEFAULT true,
    brand_regex  text,                      -- 品牌判定正则(ECOVACS 的机器常只写 WINBOT)
    match_regex  text,                      -- 型号判定正则(带词边界)，见文件头设计要点
    notes        text
);

-- 已建表的实例前向补列(本迁移每次 deploy 重放，必须两条路径都成立)
ALTER TABLE core.ci_product ADD COLUMN IF NOT EXISTS brand_regex text;
COMMENT ON TABLE core.ci_product IS
  '竞品情报产品主数据(自家 is_own=true + 竞品同表);由 db/seed/ci_product.csv 经 loader 同步';
COMMENT ON COLUMN core.ci_product.brand_regex IS
  '品牌判定正则(Python re 语法，IGNORECASE);与 match_regex 是 AND 关系 —— 商品标题常只写 WINBOT 不写 ECOVACS';
COMMENT ON COLUMN core.ci_product.match_regex IS
  '型号判定正则(Python re 语法，IGNORECASE，非 SQL ~*);只匹配型号 token，品牌交给 brand_regex。'
  'W2/W2S/W2 PRO 互为前缀，必须带 \b 词边界;命中两款以上即判为歧义，进 raw.ci_unmatched 而不是猜';

CREATE TABLE IF NOT EXISTS core.ci_product_stage (
    product_id text, brand text, model text, display_name text,
    ean text, is_own text, active text, brand_regex text, match_regex text, notes text
);
ALTER TABLE core.ci_product_stage ADD COLUMN IF NOT EXISTS brand_regex text;
COMMENT ON TABLE core.ci_product_stage IS 'CSV 装载暂存(loader 专用，每次 TRUNCATE 后 \copy 灌入;非权威)';

-- ---------------------------------------------------------------------------
-- 各源标识 → 产品(CSV 即权威)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.ci_product_alias (
    alias_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id   text NOT NULL,
    source_code  text NOT NULL,             -- 对应 core.ci_source.source_code
    external_id  text NOT NULL,             -- ASIN / idealo 商品号 / eBay item id / …
    alias_text   text,                      -- 该源上的原始标题，排查用
    match_method text NOT NULL DEFAULT 'manual'
      CHECK (match_method IN ('manual','ean','regex','llm')),
    confidence   numeric(4,3),              -- manual 恒为 1.000
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_ci_product_alias UNIQUE (source_code, external_id)
);
COMMENT ON TABLE core.ci_product_alias IS
  '各源商品标识 → 规范产品;manual 行由 db/seed/ci_product_alias.csv 同步，regex/llm 行由 flow 写入';

CREATE INDEX IF NOT EXISTS ix_cipa_product ON core.ci_product_alias (product_id);

CREATE TABLE IF NOT EXISTS core.ci_product_alias_stage (
    product_id text, source_code text, external_id text, alias_text text
);
COMMENT ON TABLE core.ci_product_alias_stage IS 'CSV 装载暂存(loader 专用);非权威';

-- ---------------------------------------------------------------------------
-- 同步函数：stage → 主数据(CSV 即权威:CSV 没有的删除、其余 upsert)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION core.sync_ci_product()
  RETURNS TABLE(removed bigint, upserted bigint, total bigint)
  LANGUAGE plpgsql AS $$
DECLARE _removed bigint; _upserted bigint;
BEGIN
    WITH d AS (
        DELETE FROM core.ci_product t
        WHERE NOT EXISTS (
            SELECT 1 FROM core.ci_product_stage s
            WHERE btrim(s.product_id) = t.product_id
        )
        RETURNING 1
    ) SELECT count(*) INTO _removed FROM d;

    WITH up AS (
        INSERT INTO core.ci_product
            (product_id, brand, model, display_name, ean, is_own, active,
             brand_regex, match_regex, notes)
        SELECT DISTINCT ON (btrim(product_id))
               btrim(product_id), btrim(brand), btrim(model), btrim(display_name),
               nullif(btrim(coalesce(ean,'')), ''),
               lower(btrim(coalesce(is_own,'false'))) IN ('true','t','1','yes'),
               lower(btrim(coalesce(active,'true')))  NOT IN ('false','f','0','no'),
               nullif(btrim(coalesce(brand_regex,'')), ''),
               nullif(btrim(coalesce(match_regex,'')), ''),
               nullif(btrim(coalesce(notes,'')), '')
        FROM core.ci_product_stage
        WHERE nullif(btrim(coalesce(product_id,'')), '') IS NOT NULL
          AND nullif(btrim(coalesce(brand,'')), '')      IS NOT NULL
          AND nullif(btrim(coalesce(model,'')), '')      IS NOT NULL
        ORDER BY btrim(product_id)
        ON CONFLICT (product_id) DO UPDATE SET
            brand        = EXCLUDED.brand,
            model        = EXCLUDED.model,
            display_name = EXCLUDED.display_name,
            ean          = EXCLUDED.ean,
            is_own       = EXCLUDED.is_own,
            active       = EXCLUDED.active,
            brand_regex  = EXCLUDED.brand_regex,
            match_regex  = EXCLUDED.match_regex,
            notes        = EXCLUDED.notes
        RETURNING 1
    ) SELECT count(*) INTO _upserted FROM up;

    RETURN QUERY
      SELECT _removed, _upserted, (SELECT count(*) FROM core.ci_product);
END
$$;
COMMENT ON FUNCTION core.sync_ci_product() IS
  '把 stage 同步进产品主数据(CSV 即权威:CSV 没有的删除、其余 upsert)，返回 删/改增/总数';

-- alias 同步只管 manual 行：flow 自动写入的 regex/llm 行不受 CSV 增删影响
CREATE OR REPLACE FUNCTION core.sync_ci_product_alias()
  RETURNS TABLE(removed bigint, upserted bigint, total bigint)
  LANGUAGE plpgsql AS $$
DECLARE _removed bigint; _upserted bigint;
BEGIN
    WITH d AS (
        DELETE FROM core.ci_product_alias t
        WHERE t.match_method = 'manual'
          AND NOT EXISTS (
            SELECT 1 FROM core.ci_product_alias_stage s
            WHERE btrim(s.source_code) = t.source_code
              AND btrim(s.external_id) = t.external_id
        )
        RETURNING 1
    ) SELECT count(*) INTO _removed FROM d;

    WITH up AS (
        INSERT INTO core.ci_product_alias
            (product_id, source_code, external_id, alias_text, match_method, confidence)
        SELECT DISTINCT ON (btrim(source_code), btrim(external_id))
               btrim(product_id), btrim(source_code), btrim(external_id),
               nullif(btrim(coalesce(alias_text,'')), ''), 'manual', 1.000
        FROM core.ci_product_alias_stage
        WHERE nullif(btrim(coalesce(source_code,'')), '') IS NOT NULL
          AND nullif(btrim(coalesce(external_id,'')), '') IS NOT NULL
          AND EXISTS (SELECT 1 FROM core.ci_product p
                      WHERE p.product_id = btrim(core.ci_product_alias_stage.product_id))
        ORDER BY btrim(source_code), btrim(external_id)
        ON CONFLICT (source_code, external_id) DO UPDATE SET
            product_id   = EXCLUDED.product_id,
            alias_text   = EXCLUDED.alias_text,
            match_method = 'manual',
            confidence   = 1.000
        RETURNING 1
    ) SELECT count(*) INTO _upserted FROM up;

    RETURN QUERY
      SELECT _removed, _upserted, (SELECT count(*) FROM core.ci_product_alias);
END
$$;
COMMENT ON FUNCTION core.sync_ci_product_alias() IS
  '把 stage 同步进 alias(只增删 match_method=manual 的行;flow 自动写的 regex/llm 行不动)';

-- ---------------------------------------------------------------------------
-- 源注册表：source_code 与 flow 代码里的字符串常量耦合，故在迁移内 seed
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.ci_source (
    source_code  text PRIMARY KEY,
    display_name text NOT NULL,
    layer        text NOT NULL CHECK (layer IN ('price','retail','social','media')),
    access_mode  text NOT NULL CHECK (access_mode IN ('api','rss','scrape')),
    base_url     text,
    active       boolean NOT NULL DEFAULT true,
    notes        text
);
COMMENT ON TABLE core.ci_source IS
  '情报源注册表;source_code 与 flows/ci_*.py 里的常量一一对应，新增源先在此登记';

INSERT INTO core.ci_source (source_code, display_name, layer, access_mode, base_url, notes) VALUES
  ('mydealz',     'mydealz.de',        'price',  'api',    'https://www.mydealz.de',      'Pepper 官方 REST /rest_api/v2;同时供 price 与 social 两条 flow 取数'),
  ('ebay',        'eBay.de',           'price',  'api',    'https://api.ebay.com',        'Browse API(OAuth client-credentials);成交价需 Marketplace Insights，未启用'),
  ('idealo',      'idealo.de',         'price',  'scrape', 'https://www.idealo.de',       'DataDome;官方 PWS 2.0 只能维护自家库存，读不到竞品'),
  ('geizhals',    'Geizhals.de',       'price',  'scrape', 'https://geizhals.de',         'Cloudflare Bot Management;v1 best-effort，失败不阻塞'),
  ('amazon_de',   'Amazon.de',         'retail', 'scrape', 'https://www.amazon.de',       '唯一销量信号源(BSR);路径白名单 /dp/{ASIN} 与 /product-reviews/{ASIN}'),
  ('mediamarkt',  'MediaMarkt.de',     'retail', 'scrape', 'https://www.mediamarkt.de',   'MMS 平台，与 saturn 共用 adapter'),
  ('saturn',      'Saturn.de',         'retail', 'scrape', 'https://www.saturn.de',       'MMS 平台，与 mediamarkt 共用 adapter'),
  ('otto',        'Otto.de',           'retail', 'scrape', 'https://www.otto.de',         'Akamai;官方 Market API 只给卖家自家数据'),
  ('reddit',      'Reddit',            'social', 'api',    'https://oauth.reddit.com',    'praw;德语相关 subreddit'),
  ('youtube',     'YouTube',           'social', 'api',    'https://www.googleapis.com',  'Data API v3;search + videos + commentThreads'),
  ('chip',        'Chip.de',           'media',  'rss',    'https://www.chip.de',         NULL),
  ('heise',       'Heise',             'media',  'rss',    'https://www.heise.de',        NULL),
  ('computerbild','Computerbild',      'media',  'rss',    'https://www.computerbild.de', NULL),
  ('connect',     'connect.de',        'media',  'rss',    'https://www.connect.de',      NULL),
  ('imtest',      'IMTEST',            'media',  'rss',    'https://www.imtest.de',       NULL),
  ('faz_kaufkompass','FAZ Kaufkompass','media',  'rss',    'https://kaufkompass.faz.net', NULL),
  ('stiwa',       'Stiftung Warentest','media',  'rss',    'https://www.test.de',         '测评正文付费墙;只记录是否有测评与结论，必要时人工补录')
ON CONFLICT (source_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 只读授权(与其它 core 参照对象一致)
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA core TO bi_readonly;
GRANT SELECT ON core.ci_product, core.ci_product_alias, core.ci_source TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON core.ci_product, core.ci_product_alias, core.ci_source,
     core.ci_product_stage, core.ci_product_alias_stage FROM bi_readonly;

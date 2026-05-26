-- ============================================================================
-- 005 — CORE GTIN 白名单：只有白名单内的 GTIN 产品进入 core（取代 003 视图链）
-- ----------------------------------------------------------------------------
-- 背景：邮箱收到的报表里混有大量“别人的产品”。raw 层按设计**忠实保留全部**
--       （审计/可追溯不动）；过滤放在 core 层单一卡点：只放行 GTIN 命中白名单
--       的行，下游 dim/fact/current 全部自动继承（删一个 GTIN 即从 BI 消失，
--       想恢复某产品只需把 GTIN 加回白名单，无需回灌 raw / 重跑 ETL）。
--
-- 产出：
--   · core.gtin_whitelist        白名单表（gtin_input 原样 + 生成列 gtin_norm 去噪）
--   · core.gtin_whitelist_stage  CSV 装载暂存（loader 用，见 db/seed/）
--   · core.norm_gtin(text)       GTIN 归一（去首尾空白/Excel .0/空格连字符）
--   · core.sync_gtin_whitelist() 把 stage 同步进白名单（CSV 即权威：增/改/删）
--   · 卡点视图 core.v_sell_through_whitelisted（keyed ⨝ 白名单），dedup 改读它
--   · core.v_gtin_unmatched      被挡在外的 GTIN（名称/数量/最近出现）→ 排查新品
--
-- 依赖：复用 002 的 core.to_int / core.to_de_date。本脚本**取代 003 的视图链**
--       （像 003 取代 002）：DROP 依赖链后重建并注入白名单 JOIN，自包含、幂等。
--
-- 重要次序：白名单为空时 core 会**空**（一切都被挡）。先把 db/seed/
--           gtin_whitelist.csv 填好 → 应用本迁移 → 跑 loader。loader 在白名单
--           为 0 条时**中止**，避免误清空 BI。详见 README「GTIN 白名单」。
--
-- 应用（幂等，可重复执行）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/005_core_gtin_whitelist.sql
-- ============================================================================

-- ---- GTIN 归一：去首尾空白 → 去 Excel 浮点尾巴 .0 → 去内部空格/连字符；空→NULL
--      纯文本变换，IMMUTABLE，可用于生成列与索引 ----
CREATE OR REPLACE FUNCTION core.norm_gtin(p text) RETURNS text
  LANGUAGE sql IMMUTABLE AS $$
    SELECT nullif(
             regexp_replace(
               regexp_replace(btrim(coalesce(p,'')), '\.0+$', ''),
               '[[:space:]-]', '', 'g'),
             '')
$$;
COMMENT ON FUNCTION core.norm_gtin(text) IS
  'GTIN 归一：去首尾空白/Excel 浮点尾巴 .0/内部空格连字符；空→NULL（白名单与匹配统一口径）';

-- ---------------------------------------------------------------------------
-- 白名单表：CSV 即权威。gtin_input 原样存（审计），gtin_norm 生成列做匹配键
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.gtin_whitelist (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gtin_input text NOT NULL,                                    -- CSV 原样（审计/纠错）
    gtin_norm  text GENERATED ALWAYS AS (core.norm_gtin(gtin_input)) STORED,
    note       text,                                             -- 备注：产品名/负责人等
    added_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_gtin_whitelist_norm UNIQUE (gtin_norm)          -- 归一后唯一（去重）
);
COMMENT ON TABLE core.gtin_whitelist IS
  'GTIN 白名单：仅其中 GTIN 的产品进入 core；由 db/seed/gtin_whitelist.csv 经 loader 同步';

-- CSV 装载暂存（loader 先 \copy 进这里，再 core.sync_gtin_whitelist() 同步）
CREATE TABLE IF NOT EXISTS core.gtin_whitelist_stage (
    gtin_input text,
    note       text
);
COMMENT ON TABLE core.gtin_whitelist_stage IS
  'CSV 装载暂存（loader 专用，每次 TRUNCATE 后 \copy 灌入；非权威）';

-- ---- 同步：CSV(stage) → 白名单。CSV 即权威：删（CSV 没有的）→ 改/增（upsert）----
CREATE OR REPLACE FUNCTION core.sync_gtin_whitelist()
  RETURNS TABLE(removed bigint, upserted bigint, total bigint)
  LANGUAGE plpgsql AS $$
DECLARE _removed bigint; _upserted bigint;
BEGIN
    WITH d AS (
        DELETE FROM core.gtin_whitelist w
        WHERE NOT EXISTS (
            SELECT 1 FROM core.gtin_whitelist_stage s
            WHERE core.norm_gtin(s.gtin_input) = w.gtin_norm
        )
        RETURNING 1
    ) SELECT count(*) INTO _removed FROM d;

    WITH up AS (
        INSERT INTO core.gtin_whitelist (gtin_input, note)
        SELECT DISTINCT ON (core.norm_gtin(gtin_input)) gtin_input, note
        FROM core.gtin_whitelist_stage
        WHERE core.norm_gtin(gtin_input) IS NOT NULL
        ORDER BY core.norm_gtin(gtin_input), gtin_input
        ON CONFLICT (gtin_norm)
          DO UPDATE SET gtin_input = EXCLUDED.gtin_input, note = EXCLUDED.note
        RETURNING 1
    ) SELECT count(*) INTO _upserted FROM up;

    RETURN QUERY
      SELECT _removed, _upserted, (SELECT count(*) FROM core.gtin_whitelist);
END
$$;
COMMENT ON FUNCTION core.sync_gtin_whitelist() IS
  '把 stage 同步进白名单（CSV 即权威：CSV 没有的删除、其余 upsert），返回 删/改增/总数';

-- ---------------------------------------------------------------------------
-- 取代 003 的视图链：DROP 依赖链后重建并注入白名单卡点（视图无数据，安全）
-- 函数 core.to_int / core.to_de_date 沿用 002；core.norm_gtin 见上。
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS
    core.fact_sell_through_current,
    core.fact_sell_through,
    core.dim_sku, core.dim_store, core.dim_supplier,
    core.v_gtin_unmatched,
    core.v_sell_through_dedup,
    core.v_sell_through_whitelisted,
    core.v_sell_through_keyed,
    core.v_sell_through_union
CASCADE;

-- ---- 多供应商联合（扩展点：每个 raw.sell_through_<供应商> 一个分支）----
CREATE VIEW core.v_sell_through_union AS
SELECT
    'EXPERT'::text AS supplier_code,
    period_flag, transaction_date, provider_name, company,
    store_id, store_name, street, postal_code, city,
    customer_sku_code, customer_sku_name, gtin_barcode, supplier_sku_code,
    sold_qty_outlets, stock_on_hand_qty_outlets,
    raw_id, source_object_key, source_file_name, source_sheet,
    source_row_number, row_hash, ingested_at
FROM raw.sell_through_expert
-- UNION ALL SELECT 'MSD'::text, ... FROM raw.sell_through_msd
;

-- ---- 解析键：德式日期 → date；从对象键提取 IMAP 收件顺序(UIDVALIDITY,UID) ----
CREATE VIEW core.v_sell_through_keyed AS
SELECT
    u.*,
    core.to_de_date(u.transaction_date) AS txn_date,
    NULLIF((regexp_match(u.source_object_key, '/([0-9]+)/([0-9]+)\.eml$'))[1], '')::bigint AS src_uidvalidity,
    NULLIF((regexp_match(u.source_object_key, '/([0-9]+)/([0-9]+)\.eml$'))[2], '')::bigint AS src_uid
FROM core.v_sell_through_union u;

-- ---- 白名单卡点：仅 GTIN（归一后）命中 core.gtin_whitelist 的行通过 ----
--      GTIN 为空 → norm=NULL → 不匹配 → 排除（无 GTIN 无法上 GTIN 白名单）。
CREATE VIEW core.v_sell_through_whitelisted AS
SELECT k.*
FROM core.v_sell_through_keyed k
JOIN core.gtin_whitelist w
  ON w.gtin_norm = core.norm_gtin(k.gtin_barcode);
COMMENT ON VIEW core.v_sell_through_whitelisted IS
  'core 单一卡点：仅 GTIN 命中白名单的行通过；下游 dedup/dim/fact 全部继承';

-- ---- 历史去重：同一(供应商,周期,日期,门店,SKU) → 最新发来的报表胜出 ----
CREATE VIEW core.v_sell_through_dedup AS
SELECT *
FROM (
    SELECT k.*,
           row_number() OVER (
               PARTITION BY supplier_code, period_flag,
                            transaction_date, store_id, customer_sku_code
               ORDER BY src_uidvalidity DESC NULLS LAST,
                        src_uid         DESC NULLS LAST,
                        ingested_at     DESC,
                        raw_id          DESC
           ) AS _rn
    FROM core.v_sell_through_whitelisted k          -- ← 已过白名单
) r
WHERE _rn = 1;
COMMENT ON VIEW core.v_sell_through_dedup IS
  '历史去重（白名单后）：同周期同键的重发/更正，按 IMAP 收件顺序取最新发来的报表';

-- ---- 维度（取每键最新发来报表的属性）----
CREATE VIEW core.dim_supplier AS
SELECT supplier_code, max(provider_name) AS supplier_name
FROM core.v_sell_through_dedup
GROUP BY supplier_code;

CREATE VIEW core.dim_store AS
SELECT DISTINCT ON (supplier_code, store_id)
       supplier_code, store_id, store_name, company, street, postal_code, city
FROM core.v_sell_through_dedup
ORDER BY supplier_code, store_id,
         src_uidvalidity DESC NULLS LAST, src_uid DESC NULLS LAST,
         ingested_at DESC, raw_id DESC;

CREATE VIEW core.dim_sku AS
SELECT DISTINCT ON (supplier_code, customer_sku_code)
       supplier_code, customer_sku_code, customer_sku_name,
       gtin_barcode, supplier_sku_code
FROM core.v_sell_through_dedup
ORDER BY supplier_code, customer_sku_code,
         src_uidvalidity DESC NULLS LAST, src_uid DESC NULLS LAST,
         ingested_at DESC, raw_id DESC;

-- ---- 历史事实：每 供应商×周期×日期×门店×SKU 一行（已去重 + 规范化）----
CREATE VIEW core.fact_sell_through AS
SELECT
    supplier_code,
    period_flag,
    txn_date                                       AS transaction_date,
    EXTRACT(ISOYEAR FROM txn_date)::int            AS period_isoyear,
    EXTRACT(WEEK    FROM txn_date)::int            AS period_isoweek,
    store_id,
    customer_sku_code,
    gtin_barcode,
    core.to_int(sold_qty_outlets)                  AS sold_qty,
    core.to_int(stock_on_hand_qty_outlets)         AS stock_on_hand_qty,
    raw_id, source_object_key, source_file_name, source_sheet,
    source_row_number, row_hash, ingested_at
FROM core.v_sell_through_dedup;
COMMENT ON VIEW core.fact_sell_through IS
  '历史事实（仅白名单 GTIN）：保留每期；同期冲突取最新发来报表；规范化(date/int/ISO周)+血缘';

-- ---- 当前快照：每 门店×SKU 取最新 transaction_date，同期取最新发来报表 ----
CREATE VIEW core.fact_sell_through_current AS
SELECT DISTINCT ON (supplier_code, store_id, customer_sku_code)
    supplier_code,
    period_flag,
    txn_date                                       AS transaction_date,
    EXTRACT(ISOYEAR FROM txn_date)::int            AS period_isoyear,
    EXTRACT(WEEK    FROM txn_date)::int            AS period_isoweek,
    store_id,
    customer_sku_code,
    gtin_barcode,
    core.to_int(sold_qty_outlets)                  AS sold_qty,
    core.to_int(stock_on_hand_qty_outlets)         AS stock_on_hand_qty,
    raw_id, source_object_key, source_file_name, source_sheet,
    source_row_number, row_hash, ingested_at
FROM core.v_sell_through_dedup
ORDER BY supplier_code, store_id, customer_sku_code,
         txn_date        DESC NULLS LAST,
         src_uidvalidity DESC NULLS LAST,
         src_uid         DESC NULLS LAST,
         ingested_at     DESC, raw_id DESC;
COMMENT ON VIEW core.fact_sell_through_current IS
  '当前库存快照（仅白名单 GTIN）：每 门店×SKU 取最新 transaction_date（同期取最新发来报表）';

-- ---------------------------------------------------------------------------
-- 可见性：被白名单挡在外的 GTIN（名称/行数/最近出现）→ 发现“其实是我们的新品”
-- 取自 keyed（白名单之前），反连白名单。raw 仍全留，随时把 GTIN 加回即恢复。
-- ---------------------------------------------------------------------------
CREATE VIEW core.v_gtin_unmatched AS
SELECT
    core.norm_gtin(k.gtin_barcode)            AS gtin_norm,
    min(k.gtin_barcode)                       AS gtin_sample,
    max(k.customer_sku_name)                  AS sku_name_sample,
    count(*)                                  AS raw_rows,
    max(k.txn_date)                           AS last_txn_date,
    max(k.ingested_at)                        AS last_ingested_at
FROM core.v_sell_through_keyed k
WHERE core.norm_gtin(k.gtin_barcode) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.gtin_whitelist w
      WHERE w.gtin_norm = core.norm_gtin(k.gtin_barcode)
  )
GROUP BY core.norm_gtin(k.gtin_barcode)
ORDER BY raw_rows DESC;
COMMENT ON VIEW core.v_gtin_unmatched IS
  '不在白名单、被挡在 core 之外的 GTIN（名称/行数/最近出现）→ 排查是否漏配自家产品';

-- ---------------------------------------------------------------------------
-- 只读授权：与 004 一致。新表/视图理论上由 channelhub 默认权限覆盖，这里显式兜底。
-- ---------------------------------------------------------------------------
GRANT SELECT ON core.gtin_whitelist, core.v_gtin_unmatched TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON core.gtin_whitelist, core.gtin_whitelist_stage FROM bi_readonly;

-- ============================================================================
-- 003 — CORE 冲突解析策略 + 当前库存视图（取代 002 的视图定义；函数沿用 002）
-- ----------------------------------------------------------------------------
-- 冲突优先级（用户定义）：
--   1) 先看 transaction_date：最新日期胜出
--   2) 同一 transaction_date：以“最新发来的报表”为准
--      —— 用 IMAP 收件顺序判定（从 source_object_key 的 .../<UIDVALIDITY>/<UID>.eml
--         提取，数字排序）。确定性强、与入库先后无关、无需改 raw / 重跑。
--      最终兜底 ingested_at / raw_id。
--
-- 产出两层：
--   · core.fact_sell_through          历史事实（按 周期/日期 保留每期；同键=同一周
--                                     的重发/更正 → 最新发来的报表胜出）→ 趋势报表
--   · core.fact_sell_through_current  当前快照（每 门店×SKU 取最新 transaction_date，
--                                     同期取最新报表）→ “现在库存以什么为准”的答案
--
-- 视图实现，幂等可重复执行：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/003_core_conflict_policy.sql
-- ============================================================================

-- 依赖链整体重建（视图无数据，安全）。函数 core.to_int / core.to_de_date 沿用 002。
DROP VIEW IF EXISTS
    core.fact_sell_through_current,
    core.fact_sell_through,
    core.dim_sku, core.dim_store, core.dim_supplier,
    core.v_sell_through_dedup,
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
    FROM core.v_sell_through_keyed k
) r
WHERE _rn = 1;
COMMENT ON VIEW core.v_sell_through_dedup IS
  '历史去重：同周期同键的重发/更正，按 IMAP 收件顺序取最新发来的报表';

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
  '历史事实：保留每期；同期冲突取最新发来的报表；规范化(date/int/ISO周)+血缘';

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
         txn_date        DESC NULLS LAST,   -- 1) 最新 transaction_date 胜出
         src_uidvalidity DESC NULLS LAST,   -- 2) 同日期：最新发来的报表
         src_uid         DESC NULLS LAST,
         ingested_at     DESC, raw_id DESC; -- 兜底
COMMENT ON VIEW core.fact_sell_through_current IS
  '当前库存快照：每 门店×SKU 取最新 transaction_date（同期取最新发来报表）';

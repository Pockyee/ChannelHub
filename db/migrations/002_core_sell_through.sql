-- ============================================================================
-- 002 — CORE 层：规范化 + 去重（视图实现，永远实时一致，幂等可重复执行）
-- ----------------------------------------------------------------------------
-- 分层：raw（忠实落地，全 TEXT，可追溯） → core（本层：规范化 + 去重）→ mart（BI）
--
-- 设计：
--   · 全部用视图：数据量小，零编排、查询即最新、无陈旧；量大再物化为表 + flow 刷新
--   · 多供应商扩展点：core.v_sell_through_union 每个 raw.sell_through_<x> 一个分支
--   · 去重：业务键 (supplier_code, period_flag, transaction_date, store_id,
--          customer_sku_code) 同键取 ingested_at 最新（raw_id 兜底）
--          → “同文件发多次”收敛为 1；“更正重发”最新版胜出；raw 全审计不动
--   · 规范化：德式 DD.MM.YYYY → date；数量 text → int（非数字→NULL）；派生 ISO 周
--   · 每条 fact 保留血缘（raw_id / source_object_key / row_hash / ingested_at）
--
-- 应用：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/002_core_sell_through.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;
COMMENT ON SCHEMA core IS '规范化 + 去重层：在 raw 之上，供 mart/BI 使用';

-- ---- 解析辅助函数（视图内复用；标 STABLE，to_date 受 locale 影响不可标 IMMUTABLE）----
CREATE OR REPLACE FUNCTION core.to_int(p text) RETURNS int
  LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN btrim(coalesce(p,'')) ~ '^-?[0-9]+$'
                THEN btrim(p)::int END
$$;
COMMENT ON FUNCTION core.to_int(text) IS '安全转整数：纯整数串才转，否则 NULL';

CREATE OR REPLACE FUNCTION core.to_de_date(p text) RETURNS date
  LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN btrim(coalesce(p,'')) ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$'
                THEN to_date(btrim(p),'DD.MM.YYYY') END
$$;
COMMENT ON FUNCTION core.to_de_date(text) IS '安全解析德式 DD.MM.YYYY → date，否则 NULL';

-- ---------------------------------------------------------------------------
-- 多供应商联合（扩展点）：每个 raw.sell_through_<供应商> 一个分支，统一形状
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sell_through_union AS
SELECT
    'EXPERT'::text              AS supplier_code,
    period_flag, transaction_date, provider_name, company,
    store_id, store_name, street, postal_code, city,
    customer_sku_code, customer_sku_name, gtin_barcode, supplier_sku_code,
    sold_qty_outlets, stock_on_hand_qty_outlets,
    raw_id, source_object_key, source_file_name, source_sheet,
    source_row_number, row_hash, ingested_at
FROM raw.sell_through_expert
-- 未来接入：
-- UNION ALL SELECT 'MSD'::text, ... FROM raw.sell_through_msd
-- UNION ALL SELECT 'TELEKOM'::text, ... FROM raw.sell_through_telekom
;

-- ---------------------------------------------------------------------------
-- 去重：业务键同键取最新 ingested_at（raw_id 兜底）
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.v_sell_through_dedup AS
SELECT *
FROM (
    SELECT u.*,
           row_number() OVER (
               PARTITION BY supplier_code, period_flag,
                            transaction_date, store_id, customer_sku_code
               ORDER BY ingested_at DESC, raw_id DESC
           ) AS _rn
    FROM core.v_sell_through_union u
) r
WHERE _rn = 1;
COMMENT ON VIEW core.v_sell_through_dedup IS
  '按业务键去重后的明细（同文件多发/更正重发 → 最新版 1 条），保留血缘';

-- ---------------------------------------------------------------------------
-- 维度（取每个键最新属性）
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.dim_supplier AS
SELECT supplier_code, max(provider_name) AS supplier_name
FROM core.v_sell_through_dedup
GROUP BY supplier_code;

CREATE OR REPLACE VIEW core.dim_store AS
SELECT DISTINCT ON (supplier_code, store_id)
       supplier_code, store_id, store_name, company,
       street, postal_code, city
FROM core.v_sell_through_dedup
ORDER BY supplier_code, store_id, ingested_at DESC, raw_id DESC;

CREATE OR REPLACE VIEW core.dim_sku AS
SELECT DISTINCT ON (supplier_code, customer_sku_code)
       supplier_code, customer_sku_code, customer_sku_name,
       gtin_barcode, supplier_sku_code
FROM core.v_sell_through_dedup
ORDER BY supplier_code, customer_sku_code, ingested_at DESC, raw_id DESC;

-- ---------------------------------------------------------------------------
-- 事实表：去重 + 规范化（typed）+ 派生周 + 血缘
-- 粒度 = 供应商 × 周期(transaction_date) × 门店 × SKU
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW core.fact_sell_through AS
SELECT
    supplier_code,
    period_flag,
    core.to_de_date(transaction_date)                              AS transaction_date,
    EXTRACT(ISOYEAR FROM core.to_de_date(transaction_date))::int    AS period_isoyear,
    EXTRACT(WEEK    FROM core.to_de_date(transaction_date))::int    AS period_isoweek,
    store_id,
    customer_sku_code,
    gtin_barcode,
    core.to_int(sold_qty_outlets)                                  AS sold_qty,
    core.to_int(stock_on_hand_qty_outlets)                         AS stock_on_hand_qty,
    -- 血缘：可回溯到被选中的那一条 raw 行 / 源邮件
    raw_id, source_object_key, source_file_name, source_sheet,
    source_row_number, row_hash, ingested_at
FROM core.v_sell_through_dedup;
COMMENT ON VIEW core.fact_sell_through IS
  '门店 sell-through/库存事实：已去重 + 规范化（date/int/ISO周），带血缘';

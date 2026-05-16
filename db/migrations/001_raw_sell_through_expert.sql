-- ============================================================================
-- 001 — RAW 层：每供应商独立 sell-through 落地表 + 装载告警表
-- ----------------------------------------------------------------------------
-- 分层架构：
--   raw  (本层)  忠实落地，全 TEXT，不做任何转换，可完整追溯到源邮件
--   core (后续)  规范化 dim/fact，把各 raw.sell_through_* UNION 归一
--   mart (后续)  BI 聚合，供 Metabase
--
-- 设计：**每供应商一张独立 raw 表**（约定 raw.sell_through_<供应商>）。
-- 各大渠道 Excel 列结构不同，每表精确镜像该供应商文件，最忠实可追溯；
-- ETL 按 Excel 表头签名识别供应商并路由到对应表，未识别则发告警邮件。
--
-- 应用方式（postgres 卷已初始化，init 脚本不会再跑，故手动执行）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/001_raw_sell_through_expert.sql
-- 幂等：可重复执行。
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS '原始落地层：供应商文件忠实入库，全 TEXT，可追溯，规范化在 core 层';

-- 早期演进留下的通用/误名表（均空表，无数据），统一为每供应商独立表
DROP TABLE IF EXISTS raw.sell_through      CASCADE;
DROP TABLE IF EXISTS raw.expert_sell_through CASCADE;

-- ---------------------------------------------------------------------------
-- 供应商 Expert 的门店 sell-through / 库存周报（忠实镜像 Expert 文件 15 列）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.sell_through_expert (
    raw_id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- ---- 源文件业务列：与 Expert 文件表头一一对应，全部原样 TEXT ----
    period_flag                text,   -- Period Flag，'W'=Woche 周报
    transaction_date           text,   -- Transaction Date，原始字符串（德式 "26.04.2026"），未解析
    provider_name              text,   -- Provider Name，供应商 = expert Warenvertriebs GmbH
    company                    text,   -- Company，Expert 网络下运营公司（Ahaus/Bening...）
    store_id                   text,   -- StoreID，门店标识符（按标识符存，保留前导零）
    store_name                 text,   -- Store，门店法人名
    street                     text,   -- Street
    postal_code                text,   -- Postal Code，德国 PLZ
    city                       text,   -- City
    customer_sku_code          text,   -- Customer SKU Code，零售商 SKU 码
    customer_sku_name          text,   -- Customer SKU Name，品名
    gtin_barcode               text,   -- GTIN Barcode，EAN/GTIN 条码
    supplier_sku_code          text,   -- Supplier SKU Code（本批多为空）
    sold_qty_outlets           text,   -- Sold Qty Outlets，门店售出量（常空），原始字符串
    stock_on_hand_qty_outlets  text,   -- Stock on Hand Qty Outlets，门店在手库存，原始字符串

    -- ---- 血缘 / 可追溯列 ----
    source_email_message_id    text,                       -- 来源邮件 Message-ID
    source_object_key          text,                       -- MinIO 中归档 .eml 的对象键
                                                            -- email/<addr>/INBOX/<uidvalidity>/<uid>.eml
    source_file_name           text,                       -- 邮件内附件文件名（如 "KW 18 26.xlsx"）
    source_sheet               text,                       -- 工作表名
    source_row_number          integer,                    -- 在源工作表中的行号（精确回溯）
    ingestion_run_id           text,                        -- Prefect flow run id

    -- 业务列内容哈希（|| 拼接，immutable），供 core 层去重
    row_hash text GENERATED ALWAYS AS (
        md5(
            coalesce(period_flag,'')                || '|' ||
            coalesce(transaction_date,'')           || '|' ||
            coalesce(provider_name,'')              || '|' ||
            coalesce(company,'')                    || '|' ||
            coalesce(store_id,'')                   || '|' ||
            coalesce(store_name,'')                 || '|' ||
            coalesce(street,'')                     || '|' ||
            coalesce(postal_code,'')                || '|' ||
            coalesce(city,'')                       || '|' ||
            coalesce(customer_sku_code,'')          || '|' ||
            coalesce(customer_sku_name,'')          || '|' ||
            coalesce(gtin_barcode,'')               || '|' ||
            coalesce(supplier_sku_code,'')          || '|' ||
            coalesce(sold_qty_outlets,'')           || '|' ||
            coalesce(stock_on_hand_qty_outlets,'')
        )
    ) STORED,

    ingested_at timestamptz NOT NULL DEFAULT now(),

    -- 幂等：同一源文件同一物理行只入库一次（重跑解析不重复）
    CONSTRAINT uq_sell_through_expert_source_row
        UNIQUE (source_object_key, source_sheet, source_row_number)
);

COMMENT ON TABLE raw.sell_through_expert IS
  'Expert 门店 sell-through/库存周报 RAW（忠实镜像 Expert 文件，可追溯源邮件，规范化在 core 层）';

CREATE INDEX IF NOT EXISTS ix_ste_store_id     ON raw.sell_through_expert (store_id);
CREATE INDEX IF NOT EXISTS ix_ste_customer_sku ON raw.sell_through_expert (customer_sku_code);
CREATE INDEX IF NOT EXISTS ix_ste_gtin         ON raw.sell_through_expert (gtin_barcode);
CREATE INDEX IF NOT EXISTS ix_ste_row_hash     ON raw.sell_through_expert (row_hash);
CREATE INDEX IF NOT EXISTS ix_ste_source_obj   ON raw.sell_through_expert (source_object_key);
CREATE INDEX IF NOT EXISTS ix_ste_ingested_at  ON raw.sell_through_expert (ingested_at);

-- ---------------------------------------------------------------------------
-- 装载告警去重表：未识别表头的 xlsx 附件已发过告警就不再重复发
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ingest_alert (
    alert_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_object_key text NOT NULL,   -- 来源 .eml 对象键
    source_file_name  text NOT NULL,   -- 未识别的附件名
    reason            text,            -- 告警原因（如 unrecognized_header）
    header_seen       text,            -- 实际读到的表头（便于排查/新增签名）
    email_subject     text,
    email_from        text,
    alerted_at        timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_ingest_alert UNIQUE (source_object_key, source_file_name)
);

COMMENT ON TABLE raw.ingest_alert IS
  '未识别报表附件告警去重：同一(对象键,附件名)只告警一次，避免每小时刷屏';

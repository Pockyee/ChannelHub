-- ============================================================================
-- 009_mart_hutt_shop.sql — Hutt 网店(Shopify)订单 BI 口径视图
-- ----------------------------------------------------------------------------
-- 源:raw.sell_through_hutt_shop_de(Shopify 订单导出,全 text 列,1 行 = 1 订单行项)
--     源表 DDL 也在本迁移内(CREATE TABLE IF NOT EXISTS)——该表此前只在本地手建,
--     未进 git,导致服务器 deploy 重放本视图时报表不存在;现表+视图同处一处,
--     任何环境(含全新初始化)按编号应用即可,无前置依赖。
-- 目标:mart.v_hutt_shop_orders —— 类型化 + 清洗,供 Superset「Hutt Online Shop」
-- 看板取数(scripts/superset_hutt_shop_dashboard.py)。
--
-- 口径:
--   · net_total = total − refunded_amount(净收入,退款即扣;取消单若已退款则净 0)
--   · 当前数据 1 订单 = 1 行项(无多行项订单);出现多行项后,订单级金额
--     (subtotal/total/…)在 Shopify 导出中只落在首行,届时本视图需按 order_name
--     拆「订单层/行项层」两个视图 —— 先以 YAGNI 不预建。
--   · region:DE 按邮编映射联邦州(core.plz_bundesland,见 008),其余国家给国家码。
--   · shipping_zip 源数据带前导撇号(Excel 防丢零),清洗后 DE 邮编补足 5 位。
--
-- 幂等:CREATE OR REPLACE VIEW,可重复执行;已加入 superset_provision.sh 的
--       IDEMPOTENT_MIGRATIONS,每次 deploy 自动重放。
-- ============================================================================

-- 源表:Shopify 订单导出 CSV 原貌镜像(79 列全 TEXT + 血缘列)。
-- 由邮件 ETL 写入;此处建表保证任何环境先有表、视图才建得起来(空表不报错)。
CREATE TABLE IF NOT EXISTS raw.sell_through_hutt_shop_de (
    raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_name                  text,
    email                       text,
    financial_status            text,
    paid_at                     text,
    fulfillment_status          text,
    fulfilled_at                text,
    accepts_marketing           text,
    currency                    text,
    subtotal                    text,
    shipping                    text,
    taxes                       text,
    total                       text,
    discount_code               text,
    discount_amount             text,
    shipping_method             text,
    order_created_at            text,
    lineitem_quantity           text,
    lineitem_name               text,
    lineitem_price              text,
    lineitem_compare_at_price   text,
    lineitem_sku                text,
    lineitem_requires_shipping  text,
    lineitem_taxable            text,
    lineitem_fulfillment_status text,
    billing_name                text,
    billing_street              text,
    billing_address1            text,
    billing_address2            text,
    billing_company             text,
    billing_city                text,
    billing_zip                 text,
    billing_province            text,
    billing_country             text,
    billing_phone               text,
    shipping_name               text,
    shipping_street             text,
    shipping_address1           text,
    shipping_address2           text,
    shipping_company            text,
    shipping_city               text,
    shipping_zip                text,
    shipping_province           text,
    shipping_country            text,
    shipping_phone              text,
    notes                       text,
    note_attributes             text,
    cancelled_at                text,
    payment_method              text,
    payment_reference           text,
    refunded_amount             text,
    vendor                      text,
    outstanding_balance         text,
    employee                    text,
    location                    text,
    device_id                   text,
    order_id                    text,
    tags                        text,
    risk_level                  text,
    source                      text,
    lineitem_discount           text,
    tax_1_name                  text,
    tax_1_value                 text,
    tax_2_name                  text,
    tax_2_value                 text,
    tax_3_name                  text,
    tax_3_value                 text,
    tax_4_name                  text,
    tax_4_value                 text,
    tax_5_name                  text,
    tax_5_value                 text,
    phone                       text,
    receipt_number              text,
    duties                      text,
    billing_province_name       text,
    shipping_province_name      text,
    payment_id                  text,
    payment_terms_name          text,
    next_payment_due_at         text,
    payment_references          text,
    source_email_message_id     text,
    source_object_key           text,
    source_file_name            text,
    source_sheet                text,
    source_row_number           integer,
    ingestion_run_id            text,
    -- 业务列指纹(去重用);生成列须 IMMUTABLE 表达式,故用 || 链而非 concat_ws
    row_hash text GENERATED ALWAYS AS (md5(
        COALESCE(order_name, '') || '|' ||
        COALESCE(email, '') || '|' ||
        COALESCE(financial_status, '') || '|' ||
        COALESCE(paid_at, '') || '|' ||
        COALESCE(fulfillment_status, '') || '|' ||
        COALESCE(fulfilled_at, '') || '|' ||
        COALESCE(accepts_marketing, '') || '|' ||
        COALESCE(currency, '') || '|' ||
        COALESCE(subtotal, '') || '|' ||
        COALESCE(shipping, '') || '|' ||
        COALESCE(taxes, '') || '|' ||
        COALESCE(total, '') || '|' ||
        COALESCE(discount_code, '') || '|' ||
        COALESCE(discount_amount, '') || '|' ||
        COALESCE(shipping_method, '') || '|' ||
        COALESCE(order_created_at, '') || '|' ||
        COALESCE(lineitem_quantity, '') || '|' ||
        COALESCE(lineitem_name, '') || '|' ||
        COALESCE(lineitem_price, '') || '|' ||
        COALESCE(lineitem_compare_at_price, '') || '|' ||
        COALESCE(lineitem_sku, '') || '|' ||
        COALESCE(lineitem_requires_shipping, '') || '|' ||
        COALESCE(lineitem_taxable, '') || '|' ||
        COALESCE(lineitem_fulfillment_status, '') || '|' ||
        COALESCE(billing_name, '') || '|' ||
        COALESCE(billing_street, '') || '|' ||
        COALESCE(billing_address1, '') || '|' ||
        COALESCE(billing_address2, '') || '|' ||
        COALESCE(billing_company, '') || '|' ||
        COALESCE(billing_city, '') || '|' ||
        COALESCE(billing_zip, '') || '|' ||
        COALESCE(billing_province, '') || '|' ||
        COALESCE(billing_country, '') || '|' ||
        COALESCE(billing_phone, '') || '|' ||
        COALESCE(shipping_name, '') || '|' ||
        COALESCE(shipping_street, '') || '|' ||
        COALESCE(shipping_address1, '') || '|' ||
        COALESCE(shipping_address2, '') || '|' ||
        COALESCE(shipping_company, '') || '|' ||
        COALESCE(shipping_city, '') || '|' ||
        COALESCE(shipping_zip, '') || '|' ||
        COALESCE(shipping_province, '') || '|' ||
        COALESCE(shipping_country, '') || '|' ||
        COALESCE(shipping_phone, '') || '|' ||
        COALESCE(notes, '') || '|' ||
        COALESCE(note_attributes, '') || '|' ||
        COALESCE(cancelled_at, '') || '|' ||
        COALESCE(payment_method, '') || '|' ||
        COALESCE(payment_reference, '') || '|' ||
        COALESCE(refunded_amount, '') || '|' ||
        COALESCE(vendor, '') || '|' ||
        COALESCE(outstanding_balance, '') || '|' ||
        COALESCE(employee, '') || '|' ||
        COALESCE(location, '') || '|' ||
        COALESCE(device_id, '') || '|' ||
        COALESCE(order_id, '') || '|' ||
        COALESCE(tags, '') || '|' ||
        COALESCE(risk_level, '') || '|' ||
        COALESCE(source, '') || '|' ||
        COALESCE(lineitem_discount, '') || '|' ||
        COALESCE(tax_1_name, '') || '|' ||
        COALESCE(tax_1_value, '') || '|' ||
        COALESCE(tax_2_name, '') || '|' ||
        COALESCE(tax_2_value, '') || '|' ||
        COALESCE(tax_3_name, '') || '|' ||
        COALESCE(tax_3_value, '') || '|' ||
        COALESCE(tax_4_name, '') || '|' ||
        COALESCE(tax_4_value, '') || '|' ||
        COALESCE(tax_5_name, '') || '|' ||
        COALESCE(tax_5_value, '') || '|' ||
        COALESCE(phone, '') || '|' ||
        COALESCE(receipt_number, '') || '|' ||
        COALESCE(duties, '') || '|' ||
        COALESCE(billing_province_name, '') || '|' ||
        COALESCE(shipping_province_name, '') || '|' ||
        COALESCE(payment_id, '') || '|' ||
        COALESCE(payment_terms_name, '') || '|' ||
        COALESCE(next_payment_due_at, '') || '|' ||
        COALESCE(payment_references, '')
    )) STORED,
    ingested_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT uq_sell_through_hutt_shop_de_source_row
        UNIQUE (source_object_key, source_sheet, source_row_number)
);

COMMENT ON TABLE raw.sell_through_hutt_shop_de IS
    'hutt_shop_de(Shopify 在线店)订单导出 CSV 原貌镜像(79 列全 TEXT + 血缘列,可追溯源邮件,规范化留给后续 core 层)';

CREATE INDEX IF NOT EXISTS ix_sthsd_ingested_at  ON raw.sell_through_hutt_shop_de (ingested_at);
CREATE INDEX IF NOT EXISTS ix_sthsd_lineitem_sku ON raw.sell_through_hutt_shop_de (lineitem_sku);
CREATE INDEX IF NOT EXISTS ix_sthsd_order_id     ON raw.sell_through_hutt_shop_de (order_id);
CREATE INDEX IF NOT EXISTS ix_sthsd_order_name   ON raw.sell_through_hutt_shop_de (order_name);
CREATE INDEX IF NOT EXISTS ix_sthsd_row_hash     ON raw.sell_through_hutt_shop_de (row_hash);
CREATE INDEX IF NOT EXISTS ix_sthsd_source_obj   ON raw.sell_through_hutt_shop_de (source_object_key);

CREATE OR REPLACE VIEW mart.v_hutt_shop_orders AS
WITH cleaned AS (
    SELECT
        r.order_name,
        r.order_id,
        NULLIF(r.order_created_at, '')::timestamptz                  AS order_ts,
        (NULLIF(r.order_created_at, '')::timestamptz)::date          AS order_date,
        date_trunc('week', NULLIF(r.order_created_at, '')::timestamptz)::date
                                                                      AS order_week,
        r.financial_status,
        r.fulfillment_status,
        (NULLIF(r.cancelled_at, '') IS NOT NULL)                      AS is_cancelled,
        r.currency,
        NULLIF(r.payment_method, '')                                  AS payment_method,
        NULLIF(r.source, '')                                          AS order_source,
        NULLIF(r.discount_code, '')                                   AS discount_code,
        COALESCE(NULLIF(r.subtotal,         '')::numeric, 0)          AS subtotal,
        COALESCE(NULLIF(r.shipping,         '')::numeric, 0)          AS shipping_cost,
        COALESCE(NULLIF(r.taxes,            '')::numeric, 0)          AS taxes,
        COALESCE(NULLIF(r.total,            '')::numeric, 0)          AS total,
        COALESCE(NULLIF(r.discount_amount,  '')::numeric, 0)          AS discount_amount,
        COALESCE(NULLIF(r.refunded_amount,  '')::numeric, 0)          AS refunded_amount,
        NULLIF(r.lineitem_name, '')                                   AS product_name,
        NULLIF(r.lineitem_sku, '')                                    AS sku,
        COALESCE(NULLIF(r.lineitem_quantity, '')::int, 0)             AS quantity,
        NULLIF(r.lineitem_price, '')::numeric                         AS lineitem_price,
        NULLIF(r.shipping_country, '')                                AS shipping_country,
        -- 去前导撇号;DE 邮编左补零到 5 位(CSV 可能丢前导零)
        CASE
            WHEN NULLIF(r.shipping_country, '') = 'DE'
                THEN lpad(NULLIF(ltrim(r.shipping_zip, ''''), ''), 5, '0')
            ELSE NULLIF(ltrim(r.shipping_zip, ''''), '')
        END                                                           AS shipping_zip
    FROM raw.sell_through_hutt_shop_de r
)
SELECT
    c.*,
    c.total - c.refunded_amount                                       AS net_total,
    b.bundesland,
    -- 地区维:德国到联邦州,其他国家到国家码;邮编映射不上的归 DE (unmapped)
    CASE
        WHEN c.shipping_country = 'DE'
            THEN COALESCE(b.bundesland, 'DE (unmapped)')
        ELSE COALESCE(c.shipping_country, '(unknown)')
    END                                                               AS region
FROM cleaned c
LEFT JOIN core.plz_bundesland b
       ON c.shipping_country = 'DE' AND b.plz = c.shipping_zip;

GRANT USAGE  ON SCHEMA mart                TO bi_readonly;
GRANT SELECT ON mart.v_hutt_shop_orders    TO bi_readonly;

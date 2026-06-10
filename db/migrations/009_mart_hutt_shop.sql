-- ============================================================================
-- 009_mart_hutt_shop.sql — Hutt 网店(Shopify)订单 BI 口径视图
-- ----------------------------------------------------------------------------
-- 源:raw.sell_through_hutt_shop_de(Shopify 订单导出,全 text 列,1 行 = 1 订单行项)
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
--       BI_VIEW_MIGRATIONS,每次 deploy 自动重放。
-- ============================================================================

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

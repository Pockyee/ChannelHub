-- ============================================================================
-- 007 — MART PSI:Purchase / Sale / Inventory 口径视图(供 Superset PSI 看板)
-- ----------------------------------------------------------------------------
-- 分层:raw(落地) → core(规范化+去重+白名单) → mart(物化事实) → 本视图(BI 口径)
--
-- 背景:渠道报表只给我们两件事 —— 门店“售出量 S”和“在手库存 I”,**没有采购量 P**。
--       P(门店从渠道进了多少货)要由库存流水恒等式从相邻两期推出:
--
--             期末库存 = 期初库存 + 采购 − 销售
--         ⇒  采购 P = 期末库存 − 期初库存 + 销售
--                   = I(本期) − I(上期) + S(本期)
--
--       “上期”= 同一(供应商, 门店, GTIN)上一条有数据的 transaction_date
--       (用 LAG 按日期取,**不假设周连续** —— 报表存在缺周,见 KW2/KW15)。
--       某 店×品 的**第一期**没有上期 → P 无法推导,记 NULL(不臆造为 0)。
--
-- 口径与注意:
--   · S(sale_qty)、I(inventory_qty) 是**流量/存量原值**:
--       - S 是流量 → 跨期可相加(周趋势求和有意义)
--       - I 是存量(期末快照)→ **同一周跨门店/产品可加**(=该周总在手),
--         但**跨周相加无意义**。按产品/门店做“当前库存”请用 is_latest 过滤,
--         看板里产品/门店维的库存图都加 is_latest = true。
--   · P 可为负:渠道退货 / 库存被更正调低 / 跨缺口周累计,均属正常,保留原值。
--   · 读 mart.fact_sell_through(已白名单 + 按归一 GTIN 去重的历史事实),
--     故本视图随 mart.refresh_all() 刷新而最新;无需自身编排。
--
-- 应用(幂等,可重复执行):
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/007_mart_psi.sql
-- ============================================================================

CREATE OR REPLACE VIEW mart.v_psi AS
WITH base AS (
    SELECT
        f.supplier_code,
        f.company,
        f.store_id,
        f.gtin_norm,
        f.transaction_date,
        f.period_isoyear,
        f.period_isoweek,
        -- 销售 S:报表常留空 = 当周未售 → 视作 0(流量口径)
        coalesce(f.sold_qty, 0)                                   AS sale_qty,
        -- 库存 I:期末在手快照(可能为 NULL,保留)
        f.stock_on_hand_qty                                       AS inventory_qty,
        -- 上一期期末库存(同 店×品,按日期取上一条有数据的期)
        lag(f.stock_on_hand_qty) OVER w                           AS inventory_prev_qty,
        -- 标记每个 店×品 的最新一期 → 产品/门店维“当前库存”用它过滤,避免跨周求和
        (f.transaction_date = max(f.transaction_date) OVER w_all) AS is_latest
    FROM mart.fact_sell_through f
    WINDOW
        w     AS (PARTITION BY f.supplier_code, f.store_id, f.gtin_norm
                  ORDER BY f.transaction_date),
        w_all AS (PARTITION BY f.supplier_code, f.store_id, f.gtin_norm)
)
SELECT
    b.supplier_code,
    b.company,
    b.store_id,
    s.store_name,
    s.city,
    b.gtin_norm,
    coalesce(p.product_name, b.gtin_norm)  AS product_name,
    b.transaction_date,
    b.period_isoyear,
    b.period_isoweek,
    -- 采购 P:期末 − 期初 + 销售;首期无上期 → NULL(不可推导)
    CASE WHEN b.inventory_prev_qty IS NOT NULL
         THEN b.inventory_qty - b.inventory_prev_qty + b.sale_qty
    END                                    AS purchase_qty,
    b.sale_qty,
    b.inventory_qty,
    b.inventory_prev_qty,
    b.is_latest
FROM base b
LEFT JOIN mart.dim_store   s ON s.supplier_code = b.supplier_code AND s.store_id = b.store_id
LEFT JOIN mart.dim_product p ON p.gtin_norm    = b.gtin_norm;

COMMENT ON VIEW mart.v_psi IS
  'PSI 口径(供 Superset):S/I 取自 mart.fact_sell_through,P 由库存恒等式 I本期−I上期+S本期 推出;'
  '存量列 inventory_qty 跨周不可加,产品/门店维当前库存请用 is_latest=true 过滤';

-- 只读授权:与 mart 其它对象一致
GRANT SELECT ON mart.v_psi TO bi_readonly;

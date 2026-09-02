-- ============================================================================
-- 007 — MART PSI:Purchase / Sale / Inventory 口径视图(供 Superset PSI 看板)
-- ----------------------------------------------------------------------------
-- 分层:raw(落地) → core(规范化+去重+白名单) → mart(物化事实) → 本视图(BI 口径)
--
-- !! 依赖 !! 本视图末尾两列连的参照表都在**序号更大**的迁移里建:
--              · display_tier ← core.store_display_plz(011)
--              · bundesland   ← core.plz_bundesland  (008)
--            故 **008 与 011 必须先于本文件执行**(序号大但排前面)。
--            scripts/superset_provision.sh 与 scripts/initialize.sh 都已按此排序。
--            反过来,mart.v_psi_bundesland 现在只是本视图的别名 → 拆到 018,排在本文件之后。
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
--   · DOS(库存供应天数)的分母使用每个 门店×SKU 最新快照日前 4 周(28 天)的
--     销量；看板只在 is_latest=true 的行上计算:
--          DOS = SUM(当前库存) × 28 / SUM(最近4周销量)
--     因此 company/SKU 筛选后仍是正确的加权汇总，而不是逐 SKU DOS 的平均值。
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
        -- 上一期的 transaction_date(派生“距上次观测几周”,识别缺周伪峰)
        lag(f.transaction_date)  OVER w                           AS prev_txn_date,
        -- 标记每个 店×品 的最新一期 → 产品/门店维“当前库存”用它过滤,避免跨周求和
        (f.transaction_date = max(f.transaction_date) OVER w_all) AS is_latest,
        max(f.transaction_date) OVER w_all                         AS latest_transaction_date
    FROM mart.fact_sell_through f
    WINDOW
        w     AS (PARTITION BY f.supplier_code, f.store_id, f.gtin_norm
                  ORDER BY f.transaction_date),
        w_all AS (PARTITION BY f.supplier_code, f.store_id, f.gtin_norm)
), with_dos_demand AS (
    SELECT
        b.*,
        -- 每个 门店×SKU 各自以其最新快照为锚点，累计最近 28 天销量。
        -- 使用 28 个自然日而非 4 个数据行，缺周不会把较早销量误算进 DOS 分母。
        sum(CASE WHEN b.transaction_date >= b.latest_transaction_date - 27
                 THEN b.sale_qty ELSE 0 END)
            OVER (PARTITION BY b.supplier_code, b.store_id, b.gtin_norm)
                                                              AS sale_qty_last_4w
    FROM base b
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
    b.is_latest,
    -- 产品系列(SKU 之上一层):从品名提取型号 token Z1/Z7/X10…(无则归 (other))。
    -- 看板产品维按它聚合(系列在上),再下钻到 product_name(颜色变体)。
    -- 注:新列放在末尾,保证 CREATE OR REPLACE VIEW 可幂等重放(中间插列会报错)。
    coalesce(
      (regexp_match(coalesce(p.product_name, b.gtin_norm), '([XZ][0-9]{1,2})', 'i'))[1],
      '(other)'
    )                                      AS product_series,
    -- 距上次观测的周数(透明列):首期=NULL;正常连续周=1;缺周>1。
    -- P 是相邻两快照之差 —— weeks_since_prev>1 的行把多周采购累加在一周(伪峰,如缺 KW15
    -- 导致的 KW16)。看板可 weeks_since_prev=1 只看干净连续周 P,或据此标记/排除缺周。
    ((b.transaction_date - b.prev_txn_date) / 7)::int  AS weeks_since_prev,
    -- 新列追加在末尾，保持 CREATE OR REPLACE VIEW 对旧列顺序的兼容性。
    concat_ws(' · ', nullif(p.customer_sku_code, ''),
                    coalesce(p.product_name, b.gtin_norm))  AS sku,
    b.latest_transaction_date,
    b.sale_qty_last_4w,
    -- 新列追加在末尾(同上):门店邮编 + 陈列档位,供看板的 PLZ / Display 两个过滤器使用。
    -- 档位名单是人工维护的 PLZ 白名单(core.store_display_plz,见 011);名单里没有的门店
    -- —— 以及 dim_store 里 postal_code 为空的门店 —— 一律落为 Without Display,不丢行。
    s.postal_code                                   AS plz,
    coalesce(d.display_tier, 'Without Display')     AS display_tier,
    -- 新列追加在末尾(同上):门店 PLZ → 联邦州(core.plz_bundesland,008 建,CSV 即权威)。
    -- 门店缺 PLZ、或 PLZ 不在参照里 → (unknown),LEFT JOIN 不丢行 —— 看板的 Bundesland
    -- 过滤器与「Sale by Bundesland」表都读这一列(不再另建 v_psi_bundesland 数据集)。
    coalesce(pb.bundesland, '(unknown)')            AS bundesland
FROM with_dos_demand b
LEFT JOIN mart.dim_store   s ON s.supplier_code = b.supplier_code AND s.store_id = b.store_id
LEFT JOIN mart.dim_product p ON p.gtin_norm    = b.gtin_norm
LEFT JOIN core.store_display_plz d ON d.plz = s.postal_code
LEFT JOIN core.plz_bundesland    pb ON pb.plz = s.postal_code;

COMMENT ON VIEW mart.v_psi IS
  'PSI 口径(供 Superset):S/I 取自 mart.fact_sell_through,P 由库存恒等式 I本期−I上期+S本期 推出;'
  '存量列 inventory_qty 跨周不可加,产品/门店维当前库存请用 is_latest=true 过滤;'
  'DOS = SUM(当前库存)*28/SUM(每店每SKU最近4周销量),看板以 is_latest=true 计算';

-- 只读授权:与 mart 其它对象一致
GRANT SELECT ON mart.v_psi TO bi_readonly;

-- ============================================================================
-- 008 — GEO:德国邮编(PLZ)→ 联邦州(Bundesland)参照 + 按州 PSI/SO 视图
-- ----------------------------------------------------------------------------
-- 背景:渠道报表只给门店 PLZ/城市,没有联邦州。要按 Bundesland 统计 SO(Sell-Out,
--       即售出量 sale_qty)就需要 PLZ → Bundesland 映射。德国 PLZ 区(Leitregion)
--       与州界**不重合**(同一 2 位前缀常跨 2~3 个州),不能按前缀粗判 —— 故用
--       GeoNames 全量 PLZ→州参照表(db/seed/plz_bundesland.csv,每 PLZ 取众数州)。
--
-- 产出:
--   · core.plz_bundesland         参照表(plz 主键 → bundesland),CSV 即权威
--   · core.plz_bundesland_stage   CSV 装载暂存(loader 用)
--   · core.sync_plz_bundesland()  stage → 参照表(删/改增),CSV 即权威
--   · mart.v_psi_bundesland       v_psi + bundesland(门店 PLZ 连参照);BI 直用
--       —— SO 按州 = GROUP BY bundesland, SUM(sale_qty)
--
-- 依赖:mart.v_psi(007)、mart.dim_store(006)。装载见 db/seed/load_plz_bundesland.sh。
--
-- 应用(幂等,可重复执行):
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/008_geo_plz_bundesland.sql
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 参照表 + 暂存(CSV 即权威)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.plz_bundesland (
    plz        text PRIMARY KEY,             -- 5 位德国邮编
    bundesland text NOT NULL                 -- 规范化德文州名(16 州之一)
);
COMMENT ON TABLE core.plz_bundesland IS
  'PLZ→Bundesland 参照(GeoNames,每 PLZ 取众数州);由 db/seed/plz_bundesland.csv 经 loader 同步';

CREATE TABLE IF NOT EXISTS core.plz_bundesland_stage (
    plz text, bundesland text
);
COMMENT ON TABLE core.plz_bundesland_stage IS
  'CSV 装载暂存(loader 专用,每次 TRUNCATE 后 \copy 灌入;非权威)';

-- ---- 同步:CSV(stage) → 参照表。CSV 即权威:删(CSV 没有的)→ 改/增(upsert)----
CREATE OR REPLACE FUNCTION core.sync_plz_bundesland()
  RETURNS TABLE(removed bigint, upserted bigint, total bigint)
  LANGUAGE plpgsql AS $$
DECLARE _removed bigint; _upserted bigint;
BEGIN
    WITH d AS (
        DELETE FROM core.plz_bundesland t
        WHERE NOT EXISTS (
            SELECT 1 FROM core.plz_bundesland_stage s
            WHERE btrim(s.plz) = t.plz
        )
        RETURNING 1
    ) SELECT count(*) INTO _removed FROM d;

    WITH up AS (
        INSERT INTO core.plz_bundesland (plz, bundesland)
        SELECT DISTINCT ON (btrim(plz)) btrim(plz), btrim(bundesland)
        FROM core.plz_bundesland_stage
        WHERE nullif(btrim(plz), '') IS NOT NULL
          AND nullif(btrim(bundesland), '') IS NOT NULL
        ORDER BY btrim(plz), btrim(bundesland)
        ON CONFLICT (plz)
          DO UPDATE SET bundesland = EXCLUDED.bundesland
        RETURNING 1
    ) SELECT count(*) INTO _upserted FROM up;

    RETURN QUERY
      SELECT _removed, _upserted, (SELECT count(*) FROM core.plz_bundesland);
END
$$;
COMMENT ON FUNCTION core.sync_plz_bundesland() IS
  '把 stage 同步进 PLZ→州参照(CSV 即权威:CSV 没有的删除、其余 upsert),返回 删/改增/总数';

-- ---------------------------------------------------------------------------
-- BI 视图:v_psi 挂上 bundesland(门店 PLZ → 州);grain 不变(店×品×周)+ 州属性。
-- 无映射(新 PLZ 未入参照)→ (unknown),不丢行。SO 按州 = SUM(sale_qty) group by bundesland。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW mart.v_psi_bundesland AS
SELECT
    coalesce(pb.bundesland, '(unknown)') AS bundesland,
    v.*
FROM mart.v_psi v
JOIN mart.dim_store s
  ON s.supplier_code = v.supplier_code AND s.store_id = v.store_id
LEFT JOIN core.plz_bundesland pb
  ON pb.plz = s.postal_code;
COMMENT ON VIEW mart.v_psi_bundesland IS
  'PSI + Bundesland(门店 PLZ 连 core.plz_bundesland);SO 按州 = GROUP BY bundesland, SUM(sale_qty)';

-- ---------------------------------------------------------------------------
-- 只读授权(与其它 mart/core 对象一致)
-- ---------------------------------------------------------------------------
GRANT SELECT ON core.plz_bundesland TO bi_readonly;
GRANT SELECT ON mart.v_psi_bundesland TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON core.plz_bundesland, core.plz_bundesland_stage FROM bi_readonly;

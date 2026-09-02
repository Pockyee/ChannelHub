-- ============================================================================
-- 011 — 门店陈列档位(Display Tier)参照:PLZ → Big / Small Display
-- ----------------------------------------------------------------------------
-- 背景:业务方按陈列规模盯一批重点门店,分三档 Big Display / Small Display /
--       Without Display。渠道报表里没有这个属性,只能靠人工维护的 PLZ 名单打标。
--       两张 CSV 各代表一档,都没命中的门店即 Without Display(视图侧 coalesce)。
--
-- 产出:
--   · core.store_display_plz         参照表(plz 主键 → display_tier),CSV 即权威
--   · core.store_display_plz_stage   CSV 装载暂存(loader 用)
--   · core.sync_display_plz()        stage → 参照表(删/改增),CSV 即权威
--
-- 消费方:mart.v_psi(007)末尾的 plz / display_tier 两列 —— 即 Superset 看板的
--        PLZ 过滤器与 Display 过滤器。兼容别名 v_psi_bundesland(018)经 v.* 自动继承。
--
-- !! 执行顺序 !!
--   本文件序号虽然是 011,但 007_mart_psi.sql 依赖它建的 core.store_display_plz,
--   所以**必须先于 007 执行**。两条执行路径都已按此处理:
--     · scripts/superset_provision.sh —— IDEMPOTENT_MIGRATIONS 数组里排在 007 之前
--     · scripts/initialize.sh         —— glob 循环之前显式先应用一次(循环里重放无副作用)
--
-- 装载:db/seed/load_display_plz.sh(一次读 big_display_plz.csv + small_display_plz.csv)
--
-- 应用(幂等,可重复执行):
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/011_core_display_plz.sql
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 参照表 + 暂存(CSV 即权威)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.store_display_plz (
    plz          text PRIMARY KEY,           -- 5 位德国邮编
    display_tier text NOT NULL
      CHECK (display_tier IN ('Big Display', 'Small Display'))
);
COMMENT ON TABLE core.store_display_plz IS
  '门店陈列档位参照(PLZ→Big/Small Display);由 db/seed/{big,small}_display_plz.csv 经 loader 同步;'
  '表里没有的 PLZ 在 mart.v_psi 里落为 Without Display';

CREATE TABLE IF NOT EXISTS core.store_display_plz_stage (
    plz text, display_tier text
);
COMMENT ON TABLE core.store_display_plz_stage IS
  'CSV 装载暂存(loader 专用,每次 TRUNCATE 后 \copy 灌入;非权威)';

-- ---- 同步:CSV(stage) → 参照表。CSV 即权威:删(CSV 没有的)→ 改/增(upsert)----
-- 冲突规则:同一 PLZ 同时出现在两张 CSV 时 Big Display 优先
--          ('Big Display' < 'Small Display' 字典序,DISTINCT ON 取第一条)。
CREATE OR REPLACE FUNCTION core.sync_display_plz()
  RETURNS TABLE(removed bigint, upserted bigint, total bigint)
  LANGUAGE plpgsql AS $$
DECLARE _removed bigint; _upserted bigint;
BEGIN
    WITH d AS (
        DELETE FROM core.store_display_plz t
        WHERE NOT EXISTS (
            SELECT 1 FROM core.store_display_plz_stage s
            WHERE btrim(s.plz) = t.plz
        )
        RETURNING 1
    ) SELECT count(*) INTO _removed FROM d;

    WITH up AS (
        INSERT INTO core.store_display_plz (plz, display_tier)
        SELECT DISTINCT ON (btrim(plz)) btrim(plz), btrim(display_tier)
        FROM core.store_display_plz_stage
        WHERE nullif(btrim(plz), '') IS NOT NULL
          AND btrim(display_tier) IN ('Big Display', 'Small Display')
        ORDER BY btrim(plz), btrim(display_tier)
        ON CONFLICT (plz)
          DO UPDATE SET display_tier = EXCLUDED.display_tier
        RETURNING 1
    ) SELECT count(*) INTO _upserted FROM up;

    RETURN QUERY
      SELECT _removed, _upserted, (SELECT count(*) FROM core.store_display_plz);
END
$$;
COMMENT ON FUNCTION core.sync_display_plz() IS
  '把 stage 同步进陈列档位参照(CSV 即权威:CSV 没有的删除、其余 upsert;同 PLZ 冲突时 Big 优先),返回 删/改增/总数';

-- ---------------------------------------------------------------------------
-- 只读授权(与其它 core 参照对象一致)
-- ---------------------------------------------------------------------------
GRANT SELECT ON core.store_display_plz TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
  ON core.store_display_plz, core.store_display_plz_stage FROM bi_readonly;

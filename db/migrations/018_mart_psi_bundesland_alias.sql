-- ============================================================================
-- 018 — mart.v_psi_bundesland 降级为 mart.v_psi 的兼容别名
-- ----------------------------------------------------------------------------
-- 背景:bundesland 原来只挂在 v_psi_bundesland 上(008),而看板其余图都读 v_psi。
--       Superset 的原生过滤器是按**列名**下推到范围内所有图的:v_psi 没有
--       bundesland 列 → 按州筛选会让那些图直接报 "column does not exist"。
--       所以 007 已把 bundesland 并进 v_psi(LEFT JOIN core.plz_bundesland),
--       本视图随之只剩兼容意义 —— 保留给可能引用它的 SQL Lab 查询/旧数据集。
--
-- 两处口径变化(都是修正,不是回归):
--   · 旧定义 JOIN mart.dim_store 是**内连接**,门店不在维表里(如空 store_id)的
--     事实行会被悄悄丢掉,按州汇总与其它图对不上;现在走 v_psi 的 LEFT JOIN,
--     这些行落进 (unknown) 州,总量与 v_psi 一致。
--   · 列顺序:bundesland 从第一列挪到最末(v_psi 的列尾)。因为列序变了,
--     CREATE OR REPLACE VIEW 会报错,必须 DROP + CREATE(下面已幂等处理)。
--
-- 依赖:mart.v_psi(007,已含 bundesland 列)。
--
-- 应用(幂等,可重复执行):
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/018_mart_psi_bundesland_alias.sql
-- ============================================================================

DROP VIEW IF EXISTS mart.v_psi_bundesland;

CREATE VIEW mart.v_psi_bundesland AS
SELECT v.* FROM mart.v_psi v;

COMMENT ON VIEW mart.v_psi_bundesland IS
  '兼容别名 = mart.v_psi(bundesland 已并入 v_psi 末列);新查询请直接用 mart.v_psi';

GRANT SELECT ON mart.v_psi_bundesland TO bi_readonly;

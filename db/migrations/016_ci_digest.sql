-- ============================================================================
-- 016 — MART 层：LLM 情报摘要（ci-digest flow 的落库表）
-- ----------------------------------------------------------------------------
-- 依赖：013_ci_raw.sql（raw.ci_mention）、012_ci_core.sql（core.ci_product）
--
-- 产出：
--   · mart.ci_digest   每期 × 范围 的自然语言摘要（由 flows/ci_digest.py 写入）
--
-- 为什么是**表**不是视图：
--   mart 层其余对象都是 CREATE OR REPLACE VIEW，因为它们是 SQL 能算出来的口径。
--   摘要算不出来 —— 它是把一批 raw.ci_mention 送进 LLM 换回来的派生内容，
--   必须落盘保存。这是 mart 里唯一一张实体表，破例的理由仅此一条。
--
-- 幂等：唯一键 (digest_on, window_days, scope)。同一期重跑 → ON CONFLICT 覆盖，
--   不会堆出多份摘要。**摘要允许覆盖**，与 raw 层「价格绝不原地 update」的铁律
--   不冲突：raw 是观测事实（覆盖即销毁历史），本表是可随时重算的派生物。
--
-- 成本留痕：input_tokens / output_tokens / model 三列不是可有可无的装饰。
--   这是全项目唯一按次计费的外部依赖，不记账就没法回答「这功能一个月多少钱」。
--
-- 应用(幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS)：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/016_ci_digest.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.ci_digest (
    digest_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    digest_on     date NOT NULL,          -- 本期截止日（生成日）
    window_days   integer NOT NULL,       -- 回看天数，与 CI_DIGEST_WINDOW_DAYS 对应
    scope         text NOT NULL,          -- 'all' 或某个 core.ci_product.product_id
    mention_cnt   integer NOT NULL,       -- 本期喂进去多少条，0 条时不调 LLM
    source_codes  text[],                 -- 本期覆盖到哪些源，判断摘要代表性用
    summary       text NOT NULL,
    model         text NOT NULL,
    input_tokens  integer,
    output_tokens integer,
    generated_at  timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_ci_digest UNIQUE (digest_on, window_days, scope)
);

COMMENT ON TABLE mart.ci_digest IS
  'LLM 生成的竞品情报摘要;mart 层唯一实体表(摘要 SQL 算不出来，必须落盘)。同期重跑覆盖';
COMMENT ON COLUMN mart.ci_digest.scope IS
  '''all'' = 全品类总览;其余取值为 core.ci_product.product_id，一款一份';
COMMENT ON COLUMN mart.ci_digest.mention_cnt IS
  '本期送进 LLM 的提及条数;为 0 时不调用 LLM，也就不会有该期记录 —— 看板上的空档说明那周确实没声量';
COMMENT ON COLUMN mart.ci_digest.input_tokens IS
  '按次计费的成本留痕;这是全项目唯一按量付费的外部依赖，不记账就答不出月成本';

CREATE INDEX IF NOT EXISTS ix_ci_digest_on ON mart.ci_digest (digest_on DESC, scope);

-- ---------------------------------------------------------------------------
-- 只读授权（与 mart 其余对象一致）
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA mart TO bi_readonly;
GRANT SELECT ON mart.ci_digest TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON mart.ci_digest FROM bi_readonly;

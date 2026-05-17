-- ============================================================================
-- 004 — BI 只读角色（Metabase / 未来 LLM 连库用）
-- ----------------------------------------------------------------------------
-- 仅 SELECT raw + core；不含写权限、不含 public 写。密码不在本文件（追踪文件不放
-- 密钥）——应用后单独用 .env 的 BI_READONLY_PASSWORD 执行 ALTER ROLE 设置。
--
-- 应用：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/004_bi_readonly_role.sql
--   # 然后设密码（密钥来自 .env，不入库）：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v pw="$BI_READONLY_PASSWORD" -c "ALTER ROLE bi_readonly PASSWORD :'pw'"
-- 幂等：可重复执行。
-- ============================================================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_readonly') THEN
    CREATE ROLE bi_readonly LOGIN;
  END IF;
END
$$;

COMMENT ON ROLE bi_readonly IS 'BI 只读：Metabase/LLM 连库，仅 SELECT raw+core';

-- 连接 / schema 使用
GRANT CONNECT ON DATABASE channelhub TO bi_readonly;
GRANT USAGE ON SCHEMA raw  TO bi_readonly;
GRANT USAGE ON SCHEMA core TO bi_readonly;

-- 现有对象只读
GRANT SELECT ON ALL TABLES IN SCHEMA raw  TO bi_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO bi_readonly;

-- 将来由 channelhub 在 raw/core 新建的表/视图，自动授予只读
ALTER DEFAULT PRIVILEGES FOR ROLE channelhub IN SCHEMA raw
  GRANT SELECT ON TABLES TO bi_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE channelhub IN SCHEMA core
  GRANT SELECT ON TABLES TO bi_readonly;

-- 明确不授予 public 写；确保不可改数据（双保险）
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw  FROM bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA core FROM bi_readonly;

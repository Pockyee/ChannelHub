-- ============================================================================
-- 010_raw_mail_request.sql — 邮件服务（收信 → 处理 → 回信）的记账表
-- ----------------------------------------------------------------------------
-- 用于 flows/mail_service.py。对外发信必须 at-most-once（宁可漏发也不能重复
-- 轰炸对方），所以走「先占坑再发信」：
--   1) INSERT ... ON CONFLICT DO NOTHING，rowcount=0 → 这封已处理过，直接跳过
--   2) 占坑成功 → 生成附件 → 发信 → UPDATE status='replied'
--   3) 中途失败 → 行留在 'processing' 且**不自动重试**；同时发内部告警。
--      确认无误后 DELETE 掉该行再重跑 mail-service 即可重发。
--
-- 去重键用 (rule_key, source_object_key)：同一封 .eml 将来可能命中多条规则，
-- 每条规则各自记一次账、各自发一次信。
--
-- 幂等：CREATE TABLE IF NOT EXISTS，已加入 superset_provision.sh 的
--       IDEMPOTENT_MIGRATIONS，每次 deploy 自动重放。
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.mail_request (
    request_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule_key          text NOT NULL,        -- flows/mail_service.py 里 RULES 的 key
    source_object_key text NOT NULL,        -- 来源 .eml 的 MinIO 对象键
    email_message_id  text,
    email_from        text,
    email_subject     text,
    status            text NOT NULL,        -- processing / replied / dry_run / unrecognized
    detail            text,                 -- 来源附件名，或失败原因
    reply_file_name   text,                 -- 回信附件名，如 ai-sunrise-25082026.csv
    rows_out          integer,              -- 生成附件的数据行数
    claimed_at        timestamptz NOT NULL DEFAULT now(),
    replied_at        timestamptz,
    CONSTRAINT uq_mail_request UNIQUE (rule_key, source_object_key)
);

CREATE INDEX IF NOT EXISTS ix_mail_request_status ON raw.mail_request (status);
CREATE INDEX IF NOT EXISTS ix_mail_request_claimed_at ON raw.mail_request (claimed_at);

COMMENT ON TABLE raw.mail_request IS
  '邮件服务记账：一行 = 一封被规则命中的来信。先占坑再发信，保证不重复回信';
COMMENT ON COLUMN raw.mail_request.status IS
  'processing=已占坑未完成(失败时会停在这,不自动重试) / replied=已回信 / dry_run=DRY RUN 下生成但未实际发信 / unrecognized=附件未识别,已回信说明';

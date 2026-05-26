-- ============================================================================
-- 006 — MART 层:BI 口径物化为真实表 + 一键刷新函数(由 parse flow 末尾链式调用)
-- ----------------------------------------------------------------------------
-- 分层:raw(忠实落地) → core(规范化+去重+GTIN白名单,**全视图,实时**)
--       → mart(本层:**真实表**,BI 直查,定期由 ETL 刷新)
--
-- 为什么:数据量上来后,BI 每次直查 core 会重算 union→keyed→whitelist→dedup
--         (视图链)。本层把 BI 口径物化成真实表,查询快、口径稳;core 视图链
--         **保持不动**(仍是实时事实源/血缘/白名单单一卡点),mart 只是其快照。
--
-- 键(本次设计):
--   · 门店  = 渠道既定 (supplier_code, store_id)        —— 与 core 一致
--   · 产品  = 归一 GTIN gtin_norm(core.norm_gtin,全局一行)—— 产品 id 改为 GTIN
--   · 公司  = 新增运营公司维 (supplier_code, company)    —— company 空→(unknown)
--
-- 粒度变更(重要):mart 事实按 供应商×周期×日期×门店×**GTIN** 去重
--   (core 仍按 customer_sku_code,实时不变)。同店同周同 GTIN 若有多个
--   customer_sku_code,按既定冲突优先级(最新发来报表)**取一行**——按 GTIN
--   折叠,不做跨 SKU 码求和(如需合量,把 refresh_all() 改成 SUM 聚合)。
--
-- 冲突优先级沿用 003/005:历史=同键取最新发来报表(IMAP 收件序 src_uidvalidity/
--   src_uid,兜底 ingested_at/raw_id);当前快照=每键先取最新 transaction_date。
--
-- 依赖:复用 002 的 core.to_int / core.to_de_date、005 的 core.norm_gtin,
--   读 005 的卡点视图 core.v_sell_through_whitelisted(白名单后、去重前、已带
--   txn_date / src_uidvalidity / src_uid)。**不取代** core 视图链(与 003/005 不同)。
--
-- 应用(幂等,可重复执行;改表结构需先手动 DROP 对应表):
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/006_mart_materialized.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;
COMMENT ON SCHEMA mart IS
  'BI 物化层:从 core(白名单后)按 GTIN 粒度物化为真实表,由 parse flow 末尾 mart.refresh_all() 刷新';

-- ---------------------------------------------------------------------------
-- 维度
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_company (
    supplier_code text NOT NULL,
    company       text NOT NULL,                              -- 空已归一为 (unknown)
    CONSTRAINT pk_mart_dim_company PRIMARY KEY (supplier_code, company)
);
COMMENT ON TABLE mart.dim_company IS
  '运营公司维:粒度 (供应商, company);company 为空→(unknown)。门店/事实回挂此维';

-- 产品维:键 = 归一 GTIN(全局一行)。属性取最新发来报表;note 取自白名单
CREATE TABLE IF NOT EXISTS mart.dim_product (
    gtin_norm         text PRIMARY KEY,
    gtin_sample       text,                                   -- 原始 GTIN 串(最新报表样本)
    product_name      text,                                   -- customer_sku_name(最新报表)
    customer_sku_code text,                                   -- 代表性零售商 SKU 码(最新报表)
    supplier_sku_code text,
    whitelist_note    text                                    -- core.gtin_whitelist.note
);
COMMENT ON TABLE mart.dim_product IS
  '产品维:键=归一 GTIN(全局一行);属性取最新发来报表;note 取自 core.gtin_whitelist';

-- 门店维:键 = 渠道既定 (supplier_code, store_id)
CREATE TABLE IF NOT EXISTS mart.dim_store (
    supplier_code text NOT NULL,
    store_id      text NOT NULL,
    store_name    text,
    company       text NOT NULL,                              -- → dim_company
    street        text,
    postal_code   text,
    city          text,
    CONSTRAINT pk_mart_dim_store PRIMARY KEY (supplier_code, store_id),
    CONSTRAINT fk_mart_dim_store_company
        FOREIGN KEY (supplier_code, company)
        REFERENCES mart.dim_company (supplier_code, company)
);
COMMENT ON TABLE mart.dim_store IS
  '门店维:键 (供应商, store_id 渠道既定);属性取最新发来报表;company→dim_company';

-- ---------------------------------------------------------------------------
-- 事实:历史(每 供应商×周期×日期×门店×GTIN 一行)
--   无 PK——transaction_date 解析失败可为 NULL,粒度唯一性由 refresh_all() 去重保证
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.fact_sell_through (
    supplier_code     text,
    period_flag       text,
    transaction_date  date,
    period_isoyear    int,
    period_isoweek    int,
    store_id          text,                                   -- 软连 dim_store(空 store_id 不入维)
    gtin_norm         text NOT NULL,
    company           text NOT NULL,
    customer_sku_code text,                                   -- 被选中那行的 SKU 码(可回溯)
    sold_qty          int,
    stock_on_hand_qty int,
    raw_id            bigint,
    source_object_key text,
    source_file_name  text,
    source_sheet      text,
    source_row_number int,
    row_hash          text,
    ingested_at       timestamptz,
    CONSTRAINT fk_mart_fst_product
        FOREIGN KEY (gtin_norm) REFERENCES mart.dim_product (gtin_norm),
    CONSTRAINT fk_mart_fst_company
        FOREIGN KEY (supplier_code, company)
        REFERENCES mart.dim_company (supplier_code, company)
);
COMMENT ON TABLE mart.fact_sell_through IS
  '历史事实(白名单+按归一 GTIN 去重):每 供应商×周期×日期×门店×GTIN 一行,同键取最新发来报表';
CREATE INDEX IF NOT EXISTS ix_mart_fst_store   ON mart.fact_sell_through (supplier_code, store_id);
CREATE INDEX IF NOT EXISTS ix_mart_fst_gtin    ON mart.fact_sell_through (gtin_norm);
CREATE INDEX IF NOT EXISTS ix_mart_fst_period  ON mart.fact_sell_through (period_isoyear, period_isoweek);
CREATE INDEX IF NOT EXISTS ix_mart_fst_txndate ON mart.fact_sell_through (transaction_date);

-- ---------------------------------------------------------------------------
-- 事实:当前快照(每 供应商×门店×GTIN 取最新 transaction_date,同期取最新报表)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.fact_sell_through_current (
    supplier_code     text,
    period_flag       text,
    transaction_date  date,
    period_isoyear    int,
    period_isoweek    int,
    store_id          text,
    gtin_norm         text NOT NULL,
    company           text NOT NULL,
    customer_sku_code text,
    sold_qty          int,
    stock_on_hand_qty int,
    raw_id            bigint,
    source_object_key text,
    source_file_name  text,
    source_sheet      text,
    source_row_number int,
    row_hash          text,
    ingested_at       timestamptz,
    CONSTRAINT fk_mart_fstc_product
        FOREIGN KEY (gtin_norm) REFERENCES mart.dim_product (gtin_norm),
    CONSTRAINT fk_mart_fstc_company
        FOREIGN KEY (supplier_code, company)
        REFERENCES mart.dim_company (supplier_code, company)
);
COMMENT ON TABLE mart.fact_sell_through_current IS
  '当前库存快照(白名单+按归一 GTIN):每 供应商×门店×GTIN 取最新 transaction_date(同期取最新报表)';
CREATE INDEX IF NOT EXISTS ix_mart_fstc_store ON mart.fact_sell_through_current (supplier_code, store_id);
CREATE INDEX IF NOT EXISTS ix_mart_fstc_gtin  ON mart.fact_sell_through_current (gtin_norm);

-- ---------------------------------------------------------------------------
-- 一键刷新:读 core 白名单后视图 → 临时表 → 单事务 TRUNCATE+INSERT 重建全部 mart
--   以 SELECT mart.refresh_all() 单语句调用即一个事务:读者只见旧或新,无半态。
--   返回各表行数,供 flow 日志/排查。
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION mart.refresh_all()
  RETURNS TABLE(dim_company bigint, dim_store bigint, dim_product bigint,
                fact bigint, fact_current bigint)
  LANGUAGE plpgsql AS $$
DECLARE
    c_company bigint; c_store bigint; c_product bigint; c_fact bigint; c_cur bigint;
BEGIN
    -- 只算一次 core 视图链(union→keyed→whitelist),后续 5 个 INSERT 复用
    CREATE TEMP TABLE _base ON COMMIT DROP AS
    SELECT w.*,
           core.norm_gtin(w.gtin_barcode)                       AS gtin_norm,
           coalesce(nullif(btrim(w.company), ''), '(unknown)')  AS company_key
    FROM core.v_sell_through_whitelisted w;
    -- 白名单 JOIN 已保证 gtin_norm 非空;防御性再挡一道
    DELETE FROM _base WHERE gtin_norm IS NULL;
    CREATE INDEX ON _base (supplier_code, store_id);
    CREATE INDEX ON _base (gtin_norm);

    -- 一并 TRUNCATE(同处一个 FK 图,允许整组截断);随后按依赖序回填
    TRUNCATE mart.fact_sell_through_current, mart.fact_sell_through,
             mart.dim_store, mart.dim_product, mart.dim_company;

    INSERT INTO mart.dim_company (supplier_code, company)
    SELECT DISTINCT supplier_code, company_key FROM _base;
    GET DIAGNOSTICS c_company = ROW_COUNT;

    INSERT INTO mart.dim_product
          (gtin_norm, gtin_sample, product_name,
           customer_sku_code, supplier_sku_code, whitelist_note)
    SELECT DISTINCT ON (b.gtin_norm)
           b.gtin_norm, b.gtin_barcode, b.customer_sku_name,
           b.customer_sku_code, b.supplier_sku_code, gw.note
    FROM _base b
    LEFT JOIN core.gtin_whitelist gw ON gw.gtin_norm = b.gtin_norm
    ORDER BY b.gtin_norm,
             b.src_uidvalidity DESC NULLS LAST, b.src_uid DESC NULLS LAST,
             b.ingested_at DESC, b.raw_id DESC;
    GET DIAGNOSTICS c_product = ROW_COUNT;

    INSERT INTO mart.dim_store
          (supplier_code, store_id, store_name, company, street, postal_code, city)
    SELECT DISTINCT ON (supplier_code, store_id)
           supplier_code, store_id, store_name, company_key, street, postal_code, city
    FROM _base
    WHERE nullif(btrim(store_id), '') IS NOT NULL          -- 无 store_id 非可用门店维
    ORDER BY supplier_code, store_id,
             src_uidvalidity DESC NULLS LAST, src_uid DESC NULLS LAST,
             ingested_at DESC, raw_id DESC;
    GET DIAGNOSTICS c_store = ROW_COUNT;

    -- 历史:同 供应商×周期×日期×门店×GTIN 取最新发来报表(取一行,非求和)
    INSERT INTO mart.fact_sell_through
          (supplier_code, period_flag, transaction_date, period_isoyear, period_isoweek,
           store_id, gtin_norm, company, customer_sku_code,
           sold_qty, stock_on_hand_qty,
           raw_id, source_object_key, source_file_name, source_sheet,
           source_row_number, row_hash, ingested_at)
    SELECT supplier_code, period_flag, txn_date,
           EXTRACT(ISOYEAR FROM txn_date)::int, EXTRACT(WEEK FROM txn_date)::int,
           store_id, gtin_norm, company_key, customer_sku_code,
           core.to_int(sold_qty_outlets), core.to_int(stock_on_hand_qty_outlets),
           raw_id, source_object_key, source_file_name, source_sheet,
           source_row_number, row_hash, ingested_at
    FROM (
        SELECT b.*,
               row_number() OVER (
                   PARTITION BY supplier_code, period_flag,
                                transaction_date, store_id, gtin_norm
                   ORDER BY src_uidvalidity DESC NULLS LAST,
                            src_uid         DESC NULLS LAST,
                            ingested_at     DESC, raw_id DESC
               ) AS _rn
        FROM _base b
    ) d
    WHERE _rn = 1;
    GET DIAGNOSTICS c_fact = ROW_COUNT;

    -- 当前快照:每 供应商×门店×GTIN 先取最新 transaction_date,同期取最新报表
    INSERT INTO mart.fact_sell_through_current
          (supplier_code, period_flag, transaction_date, period_isoyear, period_isoweek,
           store_id, gtin_norm, company, customer_sku_code,
           sold_qty, stock_on_hand_qty,
           raw_id, source_object_key, source_file_name, source_sheet,
           source_row_number, row_hash, ingested_at)
    SELECT DISTINCT ON (supplier_code, store_id, gtin_norm)
           supplier_code, period_flag, txn_date,
           EXTRACT(ISOYEAR FROM txn_date)::int, EXTRACT(WEEK FROM txn_date)::int,
           store_id, gtin_norm, company_key, customer_sku_code,
           core.to_int(sold_qty_outlets), core.to_int(stock_on_hand_qty_outlets),
           raw_id, source_object_key, source_file_name, source_sheet,
           source_row_number, row_hash, ingested_at
    FROM _base
    ORDER BY supplier_code, store_id, gtin_norm,
             txn_date        DESC NULLS LAST,        -- 1) 最新 transaction_date 胜出
             src_uidvalidity DESC NULLS LAST,        -- 2) 同期:最新发来报表
             src_uid         DESC NULLS LAST,
             ingested_at     DESC, raw_id DESC;      -- 兜底
    GET DIAGNOSTICS c_cur = ROW_COUNT;

    DROP TABLE _base;
    RETURN QUERY SELECT c_company, c_store, c_product, c_fact, c_cur;
END
$$;
COMMENT ON FUNCTION mart.refresh_all() IS
  '单事务重建全部 mart 表(读 core 白名单后视图,TRUNCATE+INSERT),返回 公司/门店/产品/历史/当前 行数';

-- ---------------------------------------------------------------------------
-- 只读授权:与 004 一致,为新 mart schema 显式补上(004 只覆盖 raw/core)
-- bi_readonly 仅 SELECT;不授予 refresh_all()(它会 TRUNCATE,仅 owner 调用)
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA mart TO bi_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO bi_readonly;
ALTER DEFAULT PRIVILEGES FOR ROLE channelhub IN SCHEMA mart
  GRANT SELECT ON TABLES TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA mart FROM bi_readonly;
REVOKE ALL ON FUNCTION mart.refresh_all() FROM PUBLIC;

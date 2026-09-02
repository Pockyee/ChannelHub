-- ============================================================================
-- 017 — MART 层：全量提及明细 + 类型分档（看板从「只看媒体」改为「全看+过滤」）
-- ----------------------------------------------------------------------------
-- 依赖：012_ci_core.sql、013_ci_raw.sql、014_ci_mart.sql
--
-- 产出：
--   · core.ci_source 增行 other_media（**顺带修掉一个静默丢数据的 bug，见下**）
--   · mart.v_ci_mention_detail  全部提及 + mention_kind 分档
--
-- ---------------------------------------------------------------------------
-- 为什么要登记 other_media：这不是新功能，是修 bug
-- ---------------------------------------------------------------------------
-- flows/ci_media.py:_outlet_for() 对没匹配上白名单域名的文章兜底写
-- source_code='other_media'，文档也写明「未登记的落 other_media 但仍入库」。
-- 入库确实入了 —— 但 other_media 从来没在 core.ci_source 里建过行，而
-- mart.v_ci_media_coverage 是 INNER JOIN core.ci_source 且要求 layer='media'，
-- join 不上，于是**整批在媒体看板上隐形**。
--
-- 实测受影响 31 条，且不是垃圾：Spiegel / n-tv / nextpit / WinFuture / Teltarif /
-- PCtipp 的正经评测，其中还有一篇自家 HUTT 10 的测评。登记这一行之后
-- v_ci_media_coverage 无需改动即可自动恢复（INNER JOIN 从此匹配得上）。
--
-- ---------------------------------------------------------------------------
-- mention_kind：分档规则与优先级
-- ---------------------------------------------------------------------------
--   test          实测评测 —— 标题/正文出现 im Test / Testbericht / getestet /
--                 Praxistest / ausprobiert / hands-on
--   promo         促销 —— price 层(mydealz 本质就是 deal 站)，或标题含
--                 Bestpreis / Angebot / Rabatt / 数字+€ / 折扣百分比
--   media_review  媒体报道但非实测 —— 上市消息、发布会、导购、获奖
--   discussion    用户讨论 —— social 层 + retail 层(Amazon 评论是用户内容)
--   other         兜底
--
-- !! 优先级即 CASE 顺序，test 排第一是故意的 !!
--   一篇「im Test」的评测正文里几乎必然出现价格，若 promo 先判会把大批真评测
--   吞进促销档。反过来「699 Euro auf den Markt」这类上市消息不含 test 词，
--   落 media_review，正确。
--
-- !! 正则用 \y 不是 \b !!
--   Postgres POSIX ARE 里 \y 才是词边界，\b 是**退格符**。写成 \b 不会报错，
--   只会一条都匹配不上 —— 实测这个坑让 test 档从 29 条掉到 1 条，
--   而看板上完全看不出异常。改这些正则时务必重新数一遍各档条数。
--
-- !! layer 不做 coalesce 兜底 !!
--   未登记的 source_code 一律落 other 档，让它**显眼**。若 coalesce 成 'media'，
--   下次再有人加源忘了登记，就会重演 other_media 这次的静默错分。
--
-- !! 视图列顺序 !!
--   CREATE OR REPLACE VIEW 不允许在列表中间插列，新列一律追加在末尾。
--
-- 应用(幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS)：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/017_ci_mention_detail.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- display_name 会作为 outlet 列显示在看板上，故用英文(看板是对外产物，
-- 图表名与标签一律英文;注释与 notes 保持中文，那是内部文档)。
-- ON CONFLICT 这里**故意 DO UPDATE 而非 DO NOTHING**，且只更新 display_name：
--   · display_name 是本迁移拥有的展示标签，改了要能随 deploy 生效；
--   · active 绝不能碰 —— 它是运维开关，重放覆盖会把人工停用的源又打开(见 012)。
INSERT INTO core.ci_source (source_code, display_name, layer, access_mode, base_url, notes) VALUES
  ('other_media', 'Other media (via Google News)', 'media', 'rss', NULL,
   'ci_media.py:_outlet_for() 的兜底 outlet;真实媒体名在 title 末尾。'
   '2026-09-01 补登记 —— 此前未登记导致 v_ci_media_coverage 的 INNER JOIN 静默丢掉 31 条')
ON CONFLICT (source_code) DO UPDATE SET display_name = EXCLUDED.display_name;

CREATE OR REPLACE VIEW mart.v_ci_mention_detail AS
SELECT
    m.published_at,
    m.published_at::date                                  AS published_on,
    m.ingested_at::date                                   AS ingested_on,
    date_trunc('week', coalesce(m.published_at, m.ingested_at))::date AS mention_week,
    m.product_id,
    p.brand,
    p.display_name,
    p.is_own,
    m.source_code,
    s.layer                                               AS source_layer,
    s.display_name                                        AS outlet,
    CASE
      WHEN coalesce(m.title,'') || ' ' || coalesce(left(m.body, 300),'')
           ~* '\ytests?\y|\ytestbericht|getestet|praxistest|ausprobiert|\yim check\y|hands.?on'
        THEN 'test'
      WHEN s.layer = 'price'
        OR coalesce(m.title,'') || ' ' || coalesce(left(m.body, 300),'')
           ~* 'bestpreis|angebot|deal|rabatt|reduziert|sparen|\ysale\y|aktion|tiefpreis|[0-9]+\s*€|€\s*[0-9]|[0-9]+\s*%'
        THEN 'promo'
      WHEN s.layer = 'media'              THEN 'media_review'
      WHEN s.layer IN ('social','retail') THEN 'discussion'
      ELSE 'other'
    END                                                   AS mention_kind,
    m.title,
    m.url,
    m.lang,
    length(coalesce(m.body, ''))                          AS body_len,
    m.engagement,
    -- 可点击标题：Superset 的 Table 图默认把单元格当纯文本，URL 不会变成链接。
    -- 这一列产出 <a>，配合图上的 allow_render_html=true 才能点开。
    -- !! 标题来自**抓取的外部网页**，直接当 HTML 渲染就是 XSS 入口 —— 必须在源头
    --    转义(& 要最先替换，否则会把后面替出来的实体再转一次)。Superset 的
    --    HTML_SANITIZATION 是第二道防线，不是免转义的理由。
    '<a href="' ||
      replace(replace(coalesce(m.url, ''), '&', '&amp;'), '"', '&quot;') ||
      '" target="_blank" rel="noopener noreferrer">' ||
      replace(replace(replace(replace(
        coalesce(nullif(m.title, ''), m.url, '(no title)'),
        '&', '&amp;'), '<', '&lt;'), '>', '&gt;'), '"', '&quot;') ||
      '</a>'                                              AS title_link
FROM raw.ci_mention m
JOIN core.ci_product p     ON p.product_id  = m.product_id
LEFT JOIN core.ci_source s ON s.source_code = m.source_code;

COMMENT ON VIEW mart.v_ci_mention_detail IS
  '全部提及明细(不限层) + mention_kind 分档(test/promo/media_review/discussion/other);'
  '看板按 mention_kind 过滤。v_ci_media_coverage 是它的媒体子集，保留不动';
COMMENT ON COLUMN mart.v_ci_mention_detail.mention_kind IS
  '分档规则见 017 迁移文件头;优先级 test > promo > 按 layer 归类，正则务必用 \y 不是 \b';

GRANT USAGE  ON SCHEMA mart TO bi_readonly;
GRANT SELECT ON mart.v_ci_mention_detail TO bi_readonly;

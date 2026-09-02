-- ============================================================================
-- 015 — CORE 层：Instagram hashtag 观察清单（配额是稀缺资源，故建表而非塞环境变量）
-- ----------------------------------------------------------------------------
-- 依赖：012_ci_core.sql（core.ci_source）
--
-- 产出：
--   · core.ci_hashtag        hashtag 观察清单 + 解析出的 IG hashtag id 缓存 + 配额记账
--   · core.ci_source 增行     instagram（social 层，api 模式）
--
-- 为什么单独建表，而不是像 CI_MYDEALZ_GROUPS 那样用逗号分隔的环境变量：
--   1) **配额记账**。Meta 对每个 IG 账号限制「7 天滚动窗口内最多 30 个不同
--      hashtag」。超了整条线当天全废，而且不会立刻报错、只会少数据。要在本地
--      镜像一份「窗口内已用掉几个」才能在越界前主动停手并告警。环境变量存不了
--      last_queried_at 这种运行时状态。
--   2) **ig_hashtag_id 缓存**。hashtag 名 → id 要单独调一次 /ig_hashtag_search，
--      而 id 是永久不变的。缓存下来每轮省一次调用（也少一次撞配额的机会）。
--   3) **改词不必重新部署**。跟 core.ci_source.active 同样的运维开关思路：
--      换观察词是业务动作，改 active 即可，不该走一次 CI/CD。
--
-- !! 配额的真实账本在 Meta 那边，本表只是保守镜像。!!
--   Meta 的窗口起点是它自己记的（可用 /{ig-user-id}/recently_searched_hashtags
--   查），我们的 last_queried_at 只在本地记。两边可能有偏差（比如你在
--   Graph API Explorer 里手工试过几个词，那也算进 Meta 的 30 个）。因此
--   CI_INSTAGRAM_HASHTAG_BUDGET 默认取 26 而非 30，留 4 个的安全余量。
--
-- 应用(幂等，可重复执行；已加入 superset_provision.sh 的 IDEMPOTENT_MIGRATIONS)：
--   docker compose exec -T postgres psql -U channelhub -d channelhub \
--     -v ON_ERROR_STOP=1 -f - < db/migrations/015_ci_instagram.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.ci_hashtag (
    hashtag          text PRIMARY KEY,      -- 不含 '#'，一律小写（Meta 的检索不区分大小写）
    ig_hashtag_id    text,                  -- /ig_hashtag_search 解析出的永久 id，缓存
    active           boolean NOT NULL DEFAULT true,
    first_queried_at timestamptz,           -- 首次真正查询的时点，排查配额用
    last_queried_at  timestamptz,           -- 配额窗口判定依据：> now()-7d 即「窗口内，不再额外占配额」
    last_media_count integer,               -- 上轮拿回多少条，长期为 0 说明这个词该换了
    notes            text
);

COMMENT ON TABLE core.ci_hashtag IS
  'Instagram hashtag 观察清单;Meta 限每账号 7 天滚动窗口内最多 30 个不同 hashtag，故清单必须是有限且稳定的';
COMMENT ON COLUMN core.ci_hashtag.ig_hashtag_id IS
  'hashtag 名 → 永久 id 的缓存;id 不变，缓存后每轮省一次 /ig_hashtag_search 调用';
COMMENT ON COLUMN core.ci_hashtag.last_queried_at IS
  '配额记账:7 天内查过的词再查不额外占配额，没查过的才消耗一个新名额。本地记账是 Meta 账本的保守镜像';
COMMENT ON COLUMN core.ci_hashtag.last_media_count IS
  '上轮 recent_media 返回条数;连续为 0 的词是在白占配额，该换掉';

-- 观察清单 seed。**只在不存在时插入**（DO NOTHING）——这张表是运维改的，
-- 迁移每次 deploy 重放，写成 upsert 会把人工的启停和配额记账覆盖掉。
-- 选词原则：德语为主、跟擦窗机品类强相关、且是**稳定的**长期词。
-- 追热点词（一次性的）在这个配额模型下极不划算：查一个新词就烧掉 7 天里的一个名额。
INSERT INTO core.ci_hashtag (hashtag, notes) VALUES
  ('fensterputzroboter',   '品类主词(德语)，最核心的一个'),
  ('fensterreiniger',      '品类泛词，会混入清洁剂，靠型号消歧过滤'),
  ('fensterreinigung',     '行为词，声量大但相关度低'),
  ('fensterputzen',        '行为词，同上'),
  ('hobot',                '自家品牌'),
  ('winbot',               '竞品品牌(ECOVACS 的机器常只写 WINBOT)'),
  ('ecovacs',              '竞品母品牌'),
  ('windowcleaningrobot',  '英语品类词，覆盖非德语区讨论'),
  ('fensterroboter',       '品类简写变体'),
  ('putzroboter',          '泛清洁机器人，相关度中等')
ON CONFLICT (hashtag) DO NOTHING;

-- 源注册。同样 DO NOTHING：active 是运维开关，不能被迁移重放覆盖（见 012 文件头）。
INSERT INTO core.ci_source (source_code, display_name, layer, access_mode, base_url, notes) VALUES
  ('instagram', 'Instagram', 'social', 'api', 'https://graph.facebook.com',
   'Hashtag Search API;需 App Review 过 Instagram Public Content Access。'
   '限制:只能按 hashtag(无自由文本检索)、recent_media 只回 24h 内、'
   '每账号 7 天最多 30 个不同 hashtag、且**拿不到作者**(username 字段不可请求)')
ON CONFLICT (source_code) DO NOTHING;

-- ---------------------------------------------------------------------------
-- 只读授权(与其它 core 参照对象一致)
-- ---------------------------------------------------------------------------
GRANT USAGE  ON SCHEMA core TO bi_readonly;
GRANT SELECT ON core.ci_hashtag TO bi_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON core.ci_hashtag FROM bi_readonly;

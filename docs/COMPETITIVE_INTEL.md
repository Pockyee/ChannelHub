# 竞品市场情报线（Competitive Intelligence）

平台的第二条数据线。对**自家 HUTT 10** 与**竞品 ECOVACS WINBOT W3 / W2S / W2 PRO OMNI / MINI**
五款擦窗机做日频情报采集，覆盖价格、零售口碑、用户讨论、媒体评测四层，
在 Superset 出品类 Market Intelligence 看板。

自家与竞品走**完全相同的采集路径**，同在 `core.ci_product` 里以 `is_own` 区分——
因此两边口径天然可比，不依赖任何自家销售系统的数据。

---

## 一分钟上手

```bash
# 1) 迁移 + 产品主数据（deploy 时会自动重放，首次可手动跑）
bash scripts/superset_provision.sh          # 含 012–016 迁移与 seed 装载
#    或只装 seed：
bash db/seed/load_ci_product.sh

# 2) 填标识（唯一的人工前置，见下节），再跑一次 loader
vim db/seed/ci_product_alias.csv && bash db/seed/load_ci_product.sh

# 3) 干跑验证（不写库）
docker compose run --rm --entrypoint python prefect-worker /app/flows/ci_price.py

# 4) 确认无误后关掉干跑
sed -i 's/^CI_DRY_RUN=.*/CI_DRY_RUN=false/' .env && docker compose up -d prefect-worker
```

## 唯一的人工前置：填各源商品标识

`db/seed/ci_product.csv` 已含四款产品与消歧正则，**开箱可用**。
但靠稳定 id 直接取数的源需要人工填一次 `db/seed/ci_product_alias.csv`：

| source_code | external_id 填什么 | 去哪找 |
|---|---|---|
| `amazon_de` | ASIN（10 位） | 商品页 URL `amazon.de/dp/`**`B0XXXXXXXX`** |
| `idealo` | **完整 slug 段** | `…/OffersOfProduct/`**`209453774_-winbot-w3-omni-ecovacs`**`.html` |
| `geizhals` | **完整 slug 段** | `geizhals.de/`**`ecovacs-winbot-w3-omni-fensterreinigungsroboter-a3725054`**`.html` |
| `mediamarkt` / `saturn` / `otto` | 商品号，或**整条 URL** | 见下 |

> ⚠️ Geizhals **不能用短号** `a3772143`：实测返回 403，必须用长 slug。
> eBay 与 mydealz **不需要 alias**（走关键词搜索，结果再逐条复核型号），
> 所以 loader 的缺口清单不列它们。

约定：**`external_id` 以 `http` 开头就直接当完整 URL 用**。Otto 这类带 slug 的脏 URL
直接贴整条链接即可，不必去凑模板。

填完重跑 `bash db/seed/load_ci_product.sh`，它会打印还缺哪些「产品 × 源」组合——
缺一个就等于该源上少一款产品，不会报错，只会静默少数据，所以这份清单要清空。

---

## 各源通道与难度

状态以**实测**为准（2026-08-30）：

| 源 | 通道 | 实测状态 | 说明 |
|---|---|---|---|
| **Geizhals** | 抓取（专用解析器） | ✅ **可用**，实测 5 款共 109 条报价 | 详见下节；**external_id 必须用长 slug** |
| YouTube | Data API v3 | ✅ 需 API key（免费 10k units/天） | search + videos + commentThreads |
| Reddit | OAuth application-only | ✅ 需 client id/secret（免费） | `SUBREDDITS` 常量控制搜哪些版 |
| eBay | 官方 Browse API | ✅ 需 client id/secret（免费） | 走关键词搜索，不需要 alias |
| 媒体层 | Google News RSS 统一发现 | ✅ 可用，实测 49 条入库 | 见下「媒体层为什么不逐家配 RSS」 |
| **mydealz** | **公开 RSS 分组 feed** | ✅ **可用**，无需签名 | 实测扫 90 条帖 → 4 条促销价；见下 |
| **idealo** | 抓取 | ⏸ **已停用**（`active=false`） | Akamai 403；robots 实际**允许**该路径，是反爬拦的 |
| Amazon.de | 自建抓取 | ⏳ 待填 ASIN | **唯一的销量信号源**（BSR）。见下「Amazon 的边界」 |
| MediaMarkt / Saturn | 抓取（JSON-LD） | ⏳ 待填 alias | 同一 MMS 平台，一个 adapter 覆盖两站 |
| Otto | 抓取（JSON-LD） | ⏳ 待填 alias | 官方 Market API 只给卖家自家数据，读不到竞品 |
| **Instagram** | **官方 Hashtag Search API** | ⏳ 待过 App Review | 三条结构性限制，见下节 —— 配词前必读 |

### 为什么不给每站写 CSS 选择器

优先用 **JSON-LD `schema.org/Product`** 通用抽取。德国电商为 SEO 大多输出它，
一份实现覆盖 MMS/Otto/idealo，改版存活率远高于手写选择器。

**两个例外，都是实测发现的**：Amazon 与 **Geizhals 都不发 JSON-LD**，各有专用解析器
（`parse_amazon` / `parse_geizhals`）。某站若解析全空，日志打
`解析全空 —— 该站可能改版或需补专用解析`，并按 `parse_empty` 告警，不会静默。

### Geizhals：可用，但有两个坑

1. **external_id 必须用长 slug**，不能用短号。实测
   `geizhals.de/a3772143.html` → **403**，
   `geizhals.de/hutt-10-fensterreinigungsroboter-a3772143.html` → **200**。
2. **该站不输出 JSON-LD**，走 `parse_geizhals`。每条报价是一个
   `id="offer-index-N"` 的 div，块内有展示价 `gh_price` 和一段商家点击跟踪的内联 JS
   （带机读的 `price: '249.9'` / `merchant: 'alza.de'`，优先用它）。
   ⚠️ 真实标记的属性之间是**换行**（`<div\nclass="offer …"\nid="offer-index-0"`），
   所以解析器锚定 `id="offer-index-N"` 再回溯到 `<div`，而不是匹配 `<div class=…`
   ——任何要求「`<div` 空格 `class`」的正则都会直接落空。
   ⚠️ 一个 offer 块里有**多个** `data-merchant-name`（按钮 + 商家 logo），
   必须先按块切分再匹配，全局匹配会把价格和商家配错对。

### mydealz：绕开签名，走公开 RSS

早期设计把 mydealz 当作「官方公开 REST」——**判断错了**。Pepper 的 `/rest_api/v2`
需要应用签名，不带签名一律 `HTTP 401 {"messages":["… (signature_missing_paramter)"]}`，
文档页没写这个要求。

**但不需要那个签名。** 同一站点提供无鉴权的 RSS，且信息量足够：

```
https://www.mydealz.de/rss/gruppe/<slug>
```

每条 item 直接带 `<pepper:merchant name="eBay" price="448€"/>` —— 商家与价格都有。
默认轮询三个分组（`CI_MYDEALZ_GROUPS` 可改）：

| slug | 为什么选它 |
|---|---|
| `ecovacs` | **品牌分组**，帖子天然全是竞品，命中率最高 |
| `saugroboter` | 覆盖没被归到品牌组的帖子 |
| `haushaltsgeraete` | 兜底，HUTT 这类小品牌只能靠它 |

实测状态：`/rss/hot` 与 `/rss/gruppe/<slug>` 返回 200 XML；
`/rss/alle`、`/rss/search?q=`、`/rss/neu` 全部 404 —— **没有按关键词的搜索 feed**，
只能轮询分组再用消歧正则过滤。

⚠️ **`/rss/gruppe/ecovacs` 里绝大多数是 Deebot 扫地机**，不是擦窗机。品牌闸门会放行，
全靠型号正则挡住 —— 扫地机促销价若被算成擦窗机竞品价，会直接污染主视图的价差列。
`check_ci_matching.py` 第 9c 节用真实 feed 样本锁死了这条。

**mydealz 的价值在促销价**：Geizhals 只有常规报价，实测 mydealz 把 W3 的最低价
从 €548.99 拉到 €448、W2 PRO 从 €360.92 拉到 €300 —— 促销才是真正的竞争动作。

robots.txt 对 `User-agent: *` 允许 `/rss/` 与 `/deals/`。该站另行单独禁止了一批
AI 训练爬虫（GPTBot / ClaudeBot / anthropic-ai 等）；本采集不属于那一类 ——
用途是比价监控，不是建训练语料，也不得用于模型训练。

**如果确实要拿 REST 签名**：那是 Pepper 自家 App 用的接口，凭据只能向 Pepper
（mydealz 运营方 Pepper Media Group）走商务/合作渠道申请，没有公开自助注册。
不要去逆向 App 里的签名密钥 —— 那是绕过访问控制，违反 ToS。鉴于 RSS 已经能拿到
商家、价格、链接与时间，申请签名的性价比也不高。

### idealo：已停用

`core.ci_source.active = false`。robots.txt **允许** `/preisvergleich/OffersOfProduct/`
（实测 `can_fetch=True`），挡住我们的是 Akamai Bot Manager：所有请求 403，
`safari17_2_ios` 指纹能拿到 200 但只是个 2.6KB 的 JS 挑战壳。
要通就得上 Playwright（意味着镜像从 ~200MB 涨到 ~1.5GB，按设计需同时拆独立
work pool 与 worker）或住宅代理。**已决定推迟。**

### Instagram：能用，但限制是结构性的

官方 **Hashtag Search API** 是 Meta 唯一开放的按词检索公开内容的入口。
Basic Display API 已于 2024 年底停用；`business_discovery` 只能查竞品账号的
档案与自家贴文指标，**不给别人贴文的评论正文**，对讨论层没用。

三条限制会直接影响你怎么用它，配词之前必须知道：

| 限制 | 后果 |
|---|---|
| **只能按 hashtag，没有自由文本搜索** | 「热门词」必须能落成一个 hashtag。做不了 ad-hoc 探索式检索 |
| **`recent_media` 只回查询时刻前 24 小时** | 补不了历史。漏一天就是真的少一天，所以本源失败必须告警 |
| **拿不到作者**（`username` 字段不可请求） | `author_hash` 恒为空 → `v_ci_share_of_voice.author_cnt` 对本源恒为 0 |

配额是这条线最需要经营的资源：**每账号 7 天滚动窗口内最多 30 个不同 hashtag**。
限的是 unique 数不是调用次数——窗口内查过的词再查免费，**换新词才烧名额**。
所以观察清单必须是稳定的长期词，追一次性热点在这个模型下极不划算。

观察清单在 `core.ci_hashtag`（不是环境变量），三个理由：要记配额账、要缓存
`ig_hashtag_id`、改词是运维动作不该走 CI/CD。`flows/ci_social.py:_ig_pick_hashtags()`
**先跑窗口内的老词再分配预算给新词**——反过来会把预算浪费在没验证过产出的词上，
还可能把正在跑的老词挤掉，在声量曲线上造成断点。

```sql
-- 换观察词（改数据即可，不必重新部署）
UPDATE core.ci_hashtag SET active = false WHERE hashtag = 'putzroboter';
INSERT INTO core.ci_hashtag (hashtag, notes) VALUES ('hobotlegee', '新品线')
  ON CONFLICT (hashtag) DO UPDATE SET active = true;

-- 哪些词在白占配额（连续拿不到东西）
SELECT hashtag, last_queried_at, last_media_count FROM core.ci_hashtag
WHERE active ORDER BY last_media_count NULLS FIRST;
```

`CI_INSTAGRAM_HASHTAG_BUDGET` 默认 **26 而非 30**：真实账本在 Meta 那边，你在
Graph API Explorer 里手工试的词也算进那 30 个，本地记账只是保守镜像，留 4 个余量。

**准入**是这条线唯一的重活：需要 IG Business/Creator 账号 + 绑定 FB 主页 + 应用通过
App Review 拿到 `Instagram Public Content Access` feature 与 `instagram_basic` 权限。
审核要提交用例说明和录屏，周期通常几周。`INSTAGRAM_ACCESS_TOKEN` /
`INSTAGRAM_IG_USER_ID` 任一留空则整个源跳过，不影响其它源。

### 媒体层为什么不逐家配 RSS

Chip/Heise/Computerbild/connect/imtest/FAZ Kaufkompass/Stiftung Warentest 各自的
feed 路径会改，**猜错的后果是静默漏采**（没有报错、只是永远没数据）。
改用 Google News RSS 做统一发现，再按 `<source url>` 域名归属到 `core.ci_source`
里登记的媒体源；未登记的落 `other_media` 但仍入库。

Google News 的 `<link>` 是 `news.google.com/rss/articles/…` 跳转地址，
而该路径被其 robots.txt 禁止——所以**不取全文**，用 RSS 标题+摘要入库。
这已足够支撑「哪家媒体何时评了哪款」，也就是 `mart.v_ci_media_coverage` 的口径。
想要某家的全文，把它的 feed 加进 `CI_MEDIA_EXTRA_FEEDS`（逗号分隔），
链接指向出版方真实域名时会自动走 trafilatura 抽正文。

---

## 合规约定（写在 `flows/ci_common.py` 里，不是写在文档里就算）

- **robots.txt 逐请求检查**。欧盟 DSM 第 4 条 TDM 例外依赖机器可读的 opt-out，
  所以这是技术要求而非形式。

  ⚠️ **取 robots.txt 这一步本身也必须能过反爬。** 绝不能用
  `RobotFileParser.read()`：它以 Python 默认 UA 直接 urlopen，反爬站点（idealo=Akamai）
  会回 403，而 `RobotFileParser` 按 RFC 把 401/403 解释成**禁止一切**——于是一个
  robots 实际允许的源会被永久静默关掉，日志还写着「robots.txt 禁止」，完全误导。
  `_fetch_robots_text()` 用真实 UA + TLS 指纹去取；**取不到时放行并记为未知**
  （取不到 ≠ 禁止），真正的禁止只能来自一份成功读到、且明确 Disallow 的 robots.txt。
- **每域限速** `CI_MIN_INTERVAL_SEC`（默认 2.5s）。总量本就小，慢一点换稳定得多。
- **User-Agent 带联系邮箱**（`CI_CONTACT_EMAIL`，留空回落 `ALERT_EMAIL_TO`）。
- **GDPR：作者身份只存 `author_hash`**（`CI_AUTHOR_SALT` 加盐），绝不落库显示名。
  ⚠️ 盐一旦开跑**不要再改**——改了等于历史哈希全部对不上。
- 不碰 Meta / TikTok（ToS 明确禁止）。

### Amazon 的边界

代码侧硬性路径白名单，只允许两条：

- `/dp/{ASIN}` —— 价格、评分、评论数、**BSR**
- `/product-reviews/{ASIN}` —— 评论正文

二者均**不在** amazon.de robots.txt 的 `User-agent: *` Disallow 列表内
（该表禁的是 `/dp/product-availability/`、`/dp/rate-this-item/`、
`/gp/customer-reviews/write-a-review.html` 等具体动作路径）。
搜索页 `/s?` 反爬强度高一个量级且我们已知 ASIN，代码里明确禁用；其余 `/gp/` 一律不碰。

Amazon 的 ToS 合同条款仍禁止抓取，这是合同风险而非技术风险，已由业务方明确承担。
被封时用 `CI_AMAZON_ENABLED=false` 一键停掉该源，不影响其余源当天采集；
升级路径固定为 `curl_cffi` → 住宅代理 → Keepa API（€49/月）兜底。

---

## 型号消歧：全项目唯一「错了不报错」的地方

`W2 OMNI` / `W2S OMNI` / `W2 PRO OMNI` 互为前缀（注意 **W2 OMNI 是另一款，不在跟踪范围**），
HUTT 10 与 HUTT W8 同品牌。匹配错了不会抛异常，只会悄悄把竞品数据算到自家头上。

规则（顺序即优先级，见 `ci_common.match_product`）：

1. **EAN 命中** —— 最可靠
2. **配件词命中即排除** —— `Ersatztücher`/`Zubehör`/`Nachfüll`… 不是整机
3. **品牌正则 AND 型号正则** 同时命中（品牌闸门很重要：商品标题常只写 WINBOT 不写 ECOVACS）
4. **命中两款以上 → 判为歧义，返回 None，绝不猜**

单品语境（商品页/报价）用 `match_product()`，歧义即失败；
文档语境（文章/讨论）用 `match_products_all()`，一篇对比评测同时挂到多款上——
这正是 `raw.ci_mention` 唯一键含 `product_id` 的原因。

匹配失败进 `raw.ci_unmatched` 待审队列：

```sql
SELECT * FROM raw.ci_unmatched WHERE NOT resolved ORDER BY seen_count DESC;
```

确认后补进 `db/seed/ci_product_alias.csv` 并重跑 loader。

**改了 `db/seed/ci_product.csv` 的正则，必须跑一遍消歧测试：**

```bash
docker run --rm -v "$PWD":/w -w /w channelhub-prefect-worker python scripts/check_ci_matching.py
```

它直接读发布用的那份 CSV 与 `tests/fixtures/ci/*.html`，不连库不联网。

---

## 数据模型

```
core.ci_product        产品主数据（自家 is_own=true + 竞品同表），CSV 即权威
core.ci_product_alias  各源标识 → 产品；manual 行来自 CSV，regex/llm 行由 flow 写
core.ci_source         源注册表；source_code 与 flow 里的常量一一对应

raw.ci_snapshot        抓取原样存档指针（MinIO 桶 ci-archive）
raw.ci_offer           价格观测，append-only，一天一源一商家一行
raw.ci_listing_stat    商品页日频指标（评分/评论数/BSR/在售）
raw.ci_mention         统一提及（reddit/youtube/mydealz/amazon 评论/instagram/媒体文章）
raw.ci_unmatched       消歧失败的待审队列

core.ci_hashtag        Instagram 观察清单 + ig_hashtag_id 缓存 + 配额记账
mart.ci_digest         LLM 生成的简报（mart 层唯一实体表，摘要 SQL 算不出来）
```

三条铁律：

1. **绝不原地 update 价格。** 价格历史曲线是本项目主要价值，覆盖即销毁。
   唯一例外是 `ci_mention.engagement`（播放量会长），单独用 `engagement_updated_at` 记录。
2. **`observed_on` 是实体列而非 `observed_at::date`。** UNIQUE 约束不能用表达式，
   日频幂等键必须是实体列——这是「一天一爬」能安全重试的基础。
3. **`content_hash` 去重是省钱开关。** 页面没变就不存快照、不送 LLM。

### BI 视图

| 视图 | 用途 |
|---|---|
| `mart.v_ci_compare` | **主视图**：自家 vs 竞品逐日并排 + 相对自家最优价的价差 |
| `mart.v_ci_price_daily` | 每日 × 产品 × 源的价格带（到手价 = 售价 + 运费） |
| `mart.v_ci_demand_proxy` | BSR 变化 + 评论数一阶差分 = 需求信号 |
| `mart.v_ci_share_of_voice` | 每周 × 产品 × 源的提及量与独立作者数 |
| `mart.v_ci_media_coverage` | 哪家媒体何时评了哪款（媒体层子集，保留兼容） |
| **`mart.v_ci_mention_detail`** | **【看板用】全部提及明细 + `mention_kind` 分档** |

#### `mention_kind`：一张表覆盖四类提及

看板的 "CI · Mentions" 表用它切档，不必为每一类单独建图（看板上有个
`Mention kind` 的 native filter）：

| 取值 | 含义 | 判定 |
|---|---|---|
| `test` | 实测评测 | 标题/正文含 `im Test` / `Testbericht` / `getestet` / `Praxistest` / `ausprobiert` / hands-on |
| `promo` | 促销 | price 层（mydealz 本质是 deal 站），或含 `Bestpreis` / `Angebot` / `Rabatt` / 数字+€ / 折扣百分比 |
| `media_review` | 媒体报道但非实测 | media 层且非上述两类 —— 上市消息、发布会、导购、获奖 |
| `discussion` | 用户讨论 | social 层 + retail 层（Amazon 评论是用户内容） |
| `other` | 兜底 | **source_code 没在 `core.ci_source` 登记时会落这里** |

⚠️ **`test` 排在 `promo` 前面是故意的。** 评测正文里几乎必然出现价格，
若 promo 先判会把大批真评测吞进促销档。反过来「699 Euro auf den Markt」
这类上市消息不含 test 词，正确落 `media_review`。

⚠️ **改这些正则时用 `\y` 不是 `\b`。** Postgres 的 POSIX ARE 里 `\y` 才是词边界，
`\b` 是**退格符**。写成 `\b` 不会报错，只会一条都匹配不上——实测这个坑让 `test`
档从 29 条掉到 1 条，而看板上完全看不出异常。改完务必重新数一遍各档条数：

```sql
SELECT mention_kind, count(*) FROM mart.v_ci_mention_detail GROUP BY 1 ORDER BY 2 DESC;
```

⚠️ **`source_layer` 不做 `coalesce` 兜底**，未登记的源一律落 `other` 让它显眼——
理由见下面 `other_media` 那件事。

#### 曾经踩过：`other_media` 没登记，31 条报道在媒体看板上隐形

`ci_media.py:_outlet_for()` 对没匹配上白名单域名的文章兜底写
`source_code='other_media'`，文档也写着「未登记的落 `other_media` 但仍入库」。
**入库确实入了，但没人给它在 `core.ci_source` 里建行**，而
`v_ci_media_coverage` 是 INNER JOIN `core.ci_source` 且要求 `layer='media'`
——join 不上，整批隐形。

受影响 31 条，且不是垃圾：Spiegel / n-tv / nextpit / WinFuture / Teltarif /
PCtipp 的正经评测，其中还有一篇自家 HUTT 10 的测评。017 迁移补登记这一行后，
`v_ci_media_coverage` **无需改动**即从 18 行恢复到 49 行。

教训是通用的：**INNER JOIN 到参照表的视图，参照表缺行就是静默丢数据。**
新增 `source_code` 常量时，必须同步在 `core.ci_source` 里登记。

⚠️ `v_ci_demand_proxy` 的 `lag()` 取的是**上一个有采样的日子**。采样有缺口时
（抓取失败/被封）不要直接把 delta 当日增量，先除以 `days_since_prev`。

---

## LLM 简报（`ci-digest`）

把窗口内的 `raw.ci_mention` 交给 Claude 出一份中文简报，写进 `mart.ci_digest`。
周频，周一 07:00 UTC，排在 `ci-media` 之后。

```sql
SELECT digest_on, mention_cnt, source_codes, summary FROM mart.ci_digest
ORDER BY digest_on DESC LIMIT 1;
```

三处与其它 flow 刻意不同：

1. **干跑语义不同。** 别的 flow 干跑是「照常抓取解析、不写库」，因为抓取不花钱。
   这里照常调用就是照常付费，所以 `CI_DRY_RUN=true` 时**不调 LLM**，只打印会送
   多少条、多少 token。验证取数口径不需要真的烧一次钱。
2. **不截断输入。** 超过 `CI_DIGEST_MAX_INPUT_TOKENS`（默认 40 万）时**显式失败并
   告警**，不砍数据——「基于 60% 数据写出的简报」和完整简报长得一模一样，
   没人能从结果里看出被砍过。这类错误必须在入口处暴露。
3. **零提及不写空记录。** 这个品类本来就冷，一周零声量是可能的。看板上的空档
   如实反映「那周确实没人讨论」，而不是「简报生成失败」。

**成本**：`mart.ci_digest` 记了 `model` / `input_tokens` / `output_tokens` 三列。
这是全项目唯一按量付费的外部依赖，不记账就答不出月成本。按当前量级
（周 50–200 条提及）用 Claude Opus 5 大约**每周几美分**，一年不到 5 美元。

模型固定 `claude-opus-5` + adaptive thinking。请求带了服务端拒答兜底
（`fallbacks="default"`）——本用例几乎不可能触发，留着是保险，去掉不影响功能。

---

## 排期与运维

| Flow | 排期（UTC） | 变量 |
|---|---|---|
| `ci-price` | 每日 05:00 | `CI_PRICE_CRON` |
| `ci-social` | 每日 05:30 | `CI_SOCIAL_CRON` |
| `ci-media` | 每周一 06:00 | `CI_MEDIA_CRON` |
| `ci-digest` | 每周一 07:00 | `CI_DIGEST_CRON` |

⚠️ `ci-digest` 是本项目唯一有**跨 flow 顺序依赖**的排期：它必须晚于 `ci-media`，
否则简报会漏掉当周的媒体评测。改 `CI_MEDIA_CRON` 时记得一起看这条。

**为什么是日频而不是更高频**：本项目要的是日粒度时间序列（价格变动、评论数增速、
BSR 变化），不是实时报价。提频只增加被封风险，不会让曲线更有信息量。
反过来，**delta 序列只能靠日频采样攒，补不回来**——越早开跑越值钱。

`ci-media` 周频：该品类德语评测总量极小，日频只会反复抓到同一批文章。

### 停用 / 启用某个源

`core.ci_source.active` 是**运维开关**，改数据即可，不必改代码；停用的源会被
三个 flow 直接跳过，不会每天失败一次再发一封告警邮件：

```sql
UPDATE core.ci_source SET active = false WHERE source_code = 'idealo';
```

⚠️ 迁移里**不写死** active：迁移每次 deploy 重放，写死会把人工的启停覆盖掉。

### 手动触发

```bash
docker compose exec prefect prefect deployment run 'ci-price/ci-price'
```

### 告警

复用 `raw.ingest_alert` 去重表，主题前缀 `[ChannelHub]`，同一「源 × 原因」当天只发一次。

| reason | 含义 | 处理 |
|---|---|---|
| `bot_wall` | 命中验证码/403/429 | 换 `curl_cffi` 指纹或上代理；Amazon 可先 `CI_AMAZON_ENABLED=false` |
| `parse_empty` | 页面取到但解析全空 | 多半改版：从 MinIO 取当天快照，照着修解析器 |
| `zero_matched` | 有候选但一条都没匹配上 | 消歧正则或产品主数据出问题，先跑 `check_ci_matching.py` |
| `api_auth_required` | 接口要求鉴权/签名（如 mydealz 401） | 该源不可用；**0 条结果不代表市场上没有** |
| `collect_failed` | 该源整体异常 | 看 Prefect UI 日志 |

**反爬墙必须显式失败**——`looks_like_bot_wall()` 在 200 响应里也识别验证码页，
绝不允许静默存一条空记录当作「今天没数据」，那会在曲线上留下假的下跌。

### 页面改版后怎么回溯

MinIO 桶 `ci-archive` 按 `ci/<source>/<日期>/<hash>.<ext>` 存了原始正文。
改完解析器后可以重跑历史快照，不必重爬——这正是当初设 snapshot 层的原因。

---

## 新增一个源

1. `db/migrations/012_ci_core.sql` 末尾的 `INSERT INTO core.ci_source` 加一行（幂等）
2. 该站有 JSON-LD → 把 `source_code` 加进 `flows/ci_price.py` 的 `SCRAPE_SOURCES`
   和 `URL_TEMPLATES`，不必写解析器
3. 没有 JSON-LD 或是 API → 在对应 flow 里加一个 `collect_*` task，
   注册到 flow 末尾的 jobs 列表
4. 在 `db/seed/ci_product_alias.csv` 补该源的标识
5. 存一份真实响应到 `tests/fixtures/ci/`，在 `scripts/check_ci_matching.py` 加断言

---

## 后续（尚未实现）

- **LLM 富化**：`core.ci_mention_aspect` + 固定德语 aspect 分类法
  （Schlieren / Absturzsicherung / Akkulaufzeit / Lautstärke / Preis-Leistung …），
  用 `claude-opus-5` structured outputs；历史回填走 Batch API（50% 折扣）。
  表结构已在计划中，迁移编号预留 015。
- **LLM 周报简报**：在看板之上生成可发管理层的德语/中文简报。
- Geizhals 攻坚、Playwright worker（需同时拆独立 work pool 与镜像）、
  Instagram Business Discovery、pgvector 语义聚类。

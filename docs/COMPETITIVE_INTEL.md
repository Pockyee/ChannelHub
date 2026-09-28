# 竞品市场情报线（Competitive Intelligence）

平台的第二条数据线。按「情报线」（`core.ci_product.line`）分组做日频情报采集，
覆盖价格、零售口碑、用户讨论、媒体评测、广告五层，每条线在 Superset 出一个看板：

| line | 看板（slug） | 自家 | 竞品 |
|---|---|---|---|
| `hutt` | **HUTT Competitive Intelligence**（`competitive-intel`） | HUTT 10 | ECOVACS WINBOT W3 / W2S / W2 PRO OMNI / MINI / MINI 2（擦窗机） |
| `imoo` | **imoo Competitive Intelligence**（`imoo-competitive-intel`） | imoo Z1 / Z3 / Z7 / X10 | Xplora XGO3 / X6Play、TCL Movetime MT48（儿童手表；TCL 只比价格与提及，不看广告） |

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

## 情报线（`line`）：一条线 = 一组自家 vs 竞品 = 一个看板

两条线**共用全部采集路径**（flow 不分线，逐款采集），只在出口处按 `line` 分开：

- **CSV 即权威**：`db/seed/ci_product.csv` 末列 `line`，缺省 `hutt`。
- **视图**：每个 `mart.v_ci_*` 末尾都带 `line`；看板上每张图都挂 `line == <线>` 过滤。
- **价差只在同线内算**：`v_ci_compare.price_gap_vs_own_eur` 以**同一条线里当日自家最低价**为基准。
  imoo 线有四款自家产品，基准就是其中当天最便宜的那款（通常是 Z1），不是逐款对比。
- **广告按品牌归线**（广告只有品牌没有型号），所以**一个品牌只能属于一条线**，
  `check_ci_matching.py` 第 1 节锁死了这一条。
- **简报按线各出一份**：`mart.ci_digest.scope` = 线代码，提示词里的品类与话题在
  `flows/ci_digest.py:LINES`。
- **看板**：`scripts/superset_ci_dashboard.py:DASHBOARDS` 一项一个看板；图名带线前缀
  （`HUTT CI · …` / `imoo CI · …`），脚本按图名认领已有图表，所以前缀必须互不相同。

新增一条线：CSV 加产品（填新 `line`）→ `ci_digest.py:LINES` 补品类 →
`DASHBOARDS` 加一项 → 视情况补 mydealz 分组 / subreddit → `check_ci_matching.py` 补断言。

### WINBOT MINI 分两代（2026-09-28）

`ecovacs-winbot-mini`（一代）与 `ecovacs-winbot-mini-2`（二代）是两款独立产品，价格与提及都追踪。
两条型号正则互斥：`mini` 必须**紧跟 `winbot`**（挡掉「Mini-LED TV … Ecovacs Deebot」这类合集帖），
后面紧跟 `2`（`Mini 2` / `Mini-2` / `Mini2`）的归二代，否则归一代。代价是「W3 OMNI und mini 2」
「… vs Mini Grau」这类 mini 没紧跟 winbot 的写法不会挂到 MINI 上（文章仍会挂到同时点名的其他型号）。
二代的 Geizhals 标识是 `ecovacs-winbot-mini-2-fensterreinigungsroboter-a3783716`，idealo 暂未填。

### imoo 线的现状（2026-09-28）

- **Geizhals 标识六款都已填**:Z1 / XGO3 / X6Play 用**变体总览页**(`…-vNNNNNN`,覆盖全部颜色),
  变体总览页解析出来只有一条「ab €」最低价、没有商家名(`merchant_cnt=1`),但正好是「该型号
  全颜色最低价」—— 价格走势图要的就是这个。X10(白)与 Z7(蓝,`a3945406`)是单色商品页，有逐商家
  报价，但只代表一个颜色。
- **Z3 是 Geizhals 未收录的单商家页**(`geizhals.de/6823412496`,只有 imoo-online-de 一家):
  只有纯数字 id,套 `{eid}.html` 模板会 404,所以 alias 里填的是**整条 URL**(`_url_for` 对 http
  开头的 external_id 原样使用)。
- Amazon 的 ASIN 六款都没填。Z7 蓝色的 ASIN 是 `B0CXSSLDW5`(未核对是否就是要跟踪的版本)。
- **X6Play 有两代**：Geizhals 分成「X6 Play」（`v221128`，已登记）与「X6 Play (2. Gen)」（`v210338`）。
  型号正则 `x6 play` 两代都会命中（mydealz / eBay / 媒体层），要区分的话需要把 2. Gen 拆成单独一款。
- **mydealz** 默认多轮询 `smartwatch` 与 `kinder`（Baby & Kind）两个分组；
  **Reddit** 多搜 `r/Eltern` 与 `r/smartwatch`。
- 手表配件词（`Armband für` / `Schutzfolie` / `Panzerglas` / `Hülle für` / `Ladegerät für` …）已加进
  `ACCESSORY_TOKENS`。注意旧有的 `kabel` 也会命中「inkl. Ladekabel」这类整机描述。

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
| YouTube | Data API v3 | ✅ **可用**（2026-09-28 起），key 在 `YOUTUBE_API_KEY` | search + videos + commentThreads；**只收德语视频**，见下 |
| Reddit | OAuth application-only | ✅ 需 client id/secret（免费） | `SUBREDDITS` 常量控制搜哪些版 |
| eBay | 官方 Browse API | ✅ 需 client id/secret（免费） | 走关键词搜索，不需要 alias |
| 媒体层 | Google News RSS 统一发现 | ✅ 可用，实测 49 条入库 | 见下「媒体层为什么不逐家配 RSS」 |
| **mydealz** | **公开 RSS 分组 feed** | ✅ **可用**，无需签名 | 实测扫 90 条帖 → 4 条促销价；见下 |
| **idealo** | 抓取 | ⏸ **已停用**（`active=false`） | Akamai 403；robots 实际**允许**该路径，是反爬拦的 |
| Amazon.de | 自建抓取 | ⏳ 待填 ASIN | **唯一的销量信号源**（BSR）。见下「Amazon 的边界」 |
| MediaMarkt / Saturn | 抓取（JSON-LD） | ⏳ 待填 alias | 同一 MMS 平台，一个 adapter 覆盖两站 |
| Otto | 抓取（JSON-LD） | ⏳ 待填 alias | 官方 Market API 只给卖家自家数据，读不到竞品 |
| **Instagram** | **官方 Hashtag Search API** | ⏳ 待过 App Review | 三条结构性限制，见下节 —— 配词前必读 |

### YouTube 为什么只收德语视频

`regionCode=DE` / `relevanceLanguage=de` 只影响排序，**不过滤**。2026-09-28 首跑按「最新」取，
184 条视频只有约 30 条是德语——imoo 在亚洲体量大，全球新视频（泰国配件店、imoo 越南/泰国
官方号、意大利语带货号）把德语内容挤没了。现在的规则（`flows/ci_social.py:collect_youtube`）：

- 按**相关度**检索，每款两个窗口：近一年 + 近 14 天（单次只回 25 条，新视频容易被老视频挤掉）；
- 视频声明了语言就信声明（只收 `de*`）；没声明时标题+描述里至少要有两个不同的德语常用词；
- **只收视频，不收评论**（2026-09-28 决定，看板上只看视频；已入库的 395 条评论已删）；
- 看板上 `mention_kind` 一律为 `social_media`；
- 配额约 2,450 单位/天（12 款 × 2 次 search × 100 + videos 明细），日额度 10,000。

首跑写入的 155 条非德语视频（连同评论共 507 行）已按同一规则清掉。
频道名只存 `CI_AUTHOR_SALT` 加盐哈希（GDPR），盐**一旦开跑不能再改**。

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
默认轮询六个分组（`CI_MYDEALZ_GROUPS` 可改；自定义时两条线的分组都要写上）：

| slug | 为什么选它 |
|---|---|
| `ecovacs` | **品牌分组**，帖子天然全是竞品，命中率最高 |
| `saugroboter` | 覆盖没被归到品牌组的帖子 |
| `haushaltsgeraete` | 兜底，HUTT 这类小品牌只能靠它 |
| `smartwatch` | imoo 线：以 Apple / Garmin 为主，靠品牌+型号正则过滤 |
| `kinder` | imoo 线：归到 Baby & Kind 的儿童手表帖 |
| `handyvertraege` | imoo 线：儿童手表常与运营商合约捆绑，这类帖常只挂在合约分组 |

实测状态：`/rss/hot` 与 `/rss/gruppe/<slug>` 返回 200 XML；
`/rss/alle`、`/rss/search?q=`、`/rss/neu` 全部 404 —— **没有按关键词的搜索 feed**，
只能轮询分组再用消歧正则过滤。

**合约捆绑帖只进提及、不进报价。** 「Xplora X6 Play + Vodafone Smart Tech M 1,61 €/Monat」
这类帖子上的 price 是一次性加价或合约总价，不是手表售价。标题命中
`/Monat`、`mtl.`、`Tarif`、`Vertrag`、`Allnet`、`Grundgebühr`、`Zuzahlung`、`Laufzeit`
（`ci_price.is_contract_deal`）的帖子，ci-price 跳过不写 `raw.ci_offer`；ci-social 照常作为提及入库。

**RSS 只有最新 30 条 —— 新加分组或新加产品线时要补抓一次历史。** 分组网页
`/gruppe/<slug>?page=N` 能往回翻几个月（按热度排序，页数上限见页内 `pagination.lastPage`，
越界 410；`/search` 被 robots.txt 禁止，不能用）。补抓 flow 只写提及、不写报价，
external_id 与 RSS 路径一致，重跑幂等：

```bash
docker exec -w /app/flows channelhub-prefect-worker python -c \
  "from ci_social import ci_mydealz_backfill; ci_mydealz_backfill()"
# 只补某几个分组：ci_mydealz_backfill(['smartwatch', 'handyvertraege'])
```

品牌分组（`ecovacs`）没有网页版（410），只能靠 RSS。

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

### 系列兜底：横评标题不写型号也要收（`kind='series'`）

按型号检索 + 标题/摘要里「品牌 AND 型号」判归属，会漏掉一整类最有价值的稿子：
横评和品类稿的标题通常只写「Fensterputzroboter im Test – welches Gerät hat den
Durchblick?」，不点名任何型号（Google News 的摘要又只是标题 + 媒体名）。
2026-09-16 n-tv 那篇横评就是这样漏掉的——Google 在正文里命中了检索词，
我们在标题里找不到型号，直接丢弃。

`db/seed/ci_product.csv` 里 `kind=series` 的行（目前只有 `ecovacs-winbot`，
看板显示为 `ECOVACS WINBOT (series)`）是系列兜底：

- **只有 ci-media 用。** `load_products()` 默认只返回 `kind=model`，价格/社媒/广告层
  不会拿系列名去搜；`load_ci_product.sh` 的缺标识清单也不列系列行。
- **检索：** 按 `brand + model`（即 `ECOVACS WINBOT`）搜两次——全量一次，
  外加 `when:14d` 近期窗口一次。单次检索只回 100 条且按相关度排，新稿容易被一年内的老稿挤掉。
- **归属：** 文章没命中该品牌**任何型号**时，满足其一即挂到系列行：
  1. 标题/摘要里有品牌（`brand_regex`）**且**有品类词；
  2. 由本系列检索搜回（Google 在正文里命中了系列名）**且**标题有品类词。
- **系列行的 `match_regex` 是品类词**（`winbot` / `Fenster…roboter` / `Fensterputz` / `Fensterreinig`），
  不是型号 token。这道闸挡的是同品牌的非擦窗新闻（Deebot / GOAT 割草机 / 泳池机）。
- 「命中型号」按品牌算：「Hutt ist Testsieger vor Ecovacs」挂上 `hutt-10` 的同时，
  ECOVACS 一款没点名，照样挂 ECOVACS 系列。未跟踪的型号（如 W2 OMNI）也落系列。
- 入库行的 `engagement.matched_by` 标 `series` / `model`，可据此区分：

```sql
SELECT published_at::date, title FROM raw.ci_mention
WHERE product_id = 'ecovacs-winbot' ORDER BY published_at DESC;
```

给别的品牌加系列兜底：在 CSV 加一行 `kind=series`（`model` 填系列名，检索词就是
`brand model`），`match_regex` 填品类词，然后在 `check_ci_matching.py` 第 11 节补断言。

---

## 广告层：Meta Ad Library + Google Ads Transparency（`ci-ads`）

回答「竞品在哪投广告、投了多久、覆盖多大」。两家都只有**官方**入口，不抓网页：

| 源 | 入口 | 能拿到 | 拿不到 |
|---|---|---|---|
| `meta_ads` | Graph API `/ads_archive`（Ad Library API） | 文案、投放起止、Facebook/Instagram 等投放面、**欧盟累计覆盖人数**、定向年龄/性别/地区、受益方与付款方 | 花费、互动 |
| `google_ads` | BigQuery 公共数据集 `google_ads_transparency_center.creative_stats`（仅 EEA） | 首末展示日、**展示次数区间**、投放面（YouTube/Search/…）、素材格式、定向方式 | **文案**（点链接去透明度中心看）、花费 |

欧盟的这些字段都来自 DSA（数字服务法）的透明度义务，所以只对欧盟投放的广告公开——
查德国正好适用。落库 `raw.ci_ad`，看板用 `mart.v_ci_ad_daily` / `mart.v_ci_ad_detail`，
图为 **CI · Ad Impressions (est.)**（每个时间段的展示次数走势，按品牌分色）与
**CI · Ads**（明细，可点开原广告）。看板顶部 **Ad platform** 过滤器切 Meta / Google，
**Impressions period** 过滤器切走势图的粒度（日 / 周 / 月，默认周）。
提及与广告四张图共用 **Mentions & Ads window** 时间窗口（默认最近一年，相对今天）；
Ads 明细表按 last_shown 截，窗口内还在展示的长期广告也会列出。

**展示次数走势是估算。** 源只给每条广告整个生命周期的展示次数区间，没有逐日数据；
`v_ci_ad_daily`（020）把区间中点平均摊到首末展示日，再按所选粒度加总。所以单条广告
投放期内的起伏看不到，曲线的波动来自「哪些广告在投、各自多大、投多久」的叠加；
长期常驻的大广告会把基线垫平。只含 Google（impressions）——Meta 给的是去重覆盖人数，
跨天加总没有意义，不进这张图。仍在投的广告每天刷新后 last_shown 后移，最近几周的数会被改写。

### 开通

**Meta**（免费）：

1. developers.facebook.com 建一个应用（类型选 Business 即可）。
2. 在 facebook.com/ID 完成**身份确认**——Ad Library API 的硬前置，没做会报权限错误。
3. Graph API Explorer 选该应用生成 user token，换成 60 天长期 token，填 `META_AD_LIBRARY_TOKEN`。
   过期后 flow 告警 `api_auth_required`，换新 token 即可。

**Google**（每月前 1 TiB 扫描免费）：

1. 建一个 GCP 项目，启用 BigQuery API。
2. 建 service account，授予 **BigQuery Job User**，下载 JSON key。
3. `base64 -w0 sa-key.json` 的结果填 `GOOGLE_ADS_BQ_SA_JSON_B64`；
   查询费记到哪个项目由 `GOOGLE_ADS_BQ_PROJECT` 决定（留空用 key 里的 project_id）。

填完 `docker compose up -d prefect-worker`，手动跑一次：

```bash
docker compose exec prefect prefect deployment run 'ci-ads/ci-ads'
```

### 检索策略与广告主覆盖表

默认**按品牌名**检索，品牌与判定正则直接复用 `core.ci_product.brand` / `brand_regex`：

- Meta：`search_terms=<品牌>` 按文案命中，再要求**主页名**过品牌正则——转售商
  （如「Robot-Shop24」投的 ECOVACS 广告）会被排掉，计入 `other_advertiser`。
- Google：`advertiser_disclosed_name` 过品牌正则。

想更准就在 `db/seed/ci_ad_advertiser.csv` 登记广告主。每行的 `active` 决定含义：

| 行 | 含义 |
|---|---|
| 真实 id + `true` | **白名单**：该品牌在该源改为只按这些 id 精确取 |
| 真实 id + `false` | **黑名单**：按名检索时排除这个广告主（同名公司） |
| `*` + `false` | **禁用按名检索**：品牌名太常见，按名只会搜到同名公司时用 |

配置收窄后，下一次 `ci-ads` 会顺带删掉 `raw.ci_ad` 里不再符合的旧行——状态型表不清理
就会永远留着误报。不知道 id 先跑发现命令（只读，不写库），核对名称后把行粘进 CSV：

```bash
docker compose exec prefect-worker python flows/ci_ads.py discover            # 全部品牌
docker compose exec prefect-worker python flows/ci_ads.py discover ECOVACS    # 只看一个
bash db/seed/load_ci_ad_advertiser.sh
```

发现结果里主页名过不了品牌正则的行 `active` 标为 `false`，多半是转售商，人工决定要不要。

`*` + `false` 是**按品牌**的开关（主键含 brand），同一个源上可以有多个品牌各自禁用。
2026-09-28 起 TCL 在 Google 与 Meta 上都已禁用：TCL 是综合品牌（电视/手机等），它的广告量
不代表儿童手表，TCL MT48 只做价格与提及对比。

### 按落地页域名归属（Google，`ci_ad_domain.csv`）

有的品牌不自己开 Google 广告账户，而是走**代投代理的共用账户**——按账户白名单会把代理
名下别家的广告全算进来，而 BigQuery 数据集没有落地页字段。这类品牌在
`db/seed/ci_ad_domain.csv` 登记落地页域名（裸域名、小写），`ci-ads` 每轮：

1. 去透明度中心按域名检索（地区 = `CI_ADS_REGION`），取当前全部素材 id；
2. 只按这些素材 id 去 BigQuery 取数（数字仍全部来自官方数据集）。

登记了域名的品牌**不再按品牌名检索**；`ci_ad_advertiser.csv` 的白名单账户仍并用。
改完同样跑 `bash db/seed/load_ci_ad_advertiser.sh`（两张 CSV 一起装载）。

**第 1 步用的是透明度中心网页背后的非官方接口**（`SearchService/SearchCreatives`），
无文档、可能随 Google 改版失效。失效时（HTTP 错误、响应结构变了、响应为空、取到的条数与
接口报告的总数对不上、或原本有素材的品牌突然查到 0 条）flow **退回 `raw.ci_ad` 里该品牌已有的
素材 id** 继续刷新展示数据，不删任何行，并告警 `domain_lookup_failed`：由 `SMTP_USER`
（data@ai-sunrise.de）发邮件给 `ALERT_EMAIL_TO`（qi.bao@ai-sunrise.de），同一天只发一次。
此时新上的广告进不来，需按邮件里的报错修 `flows/ci_ads.py:parse_tc_page`。

2026-09-28 登记的 imoo（用户告知广告主是 BlueVision Interactive Limited）：

- **imoostore.com** → 93 条素材，全在 BlueVision 账户 `AR10942234166510485505` 下。
  这是**代理共用账户**：德国 6.7 万条素材、46 个类目，imoo 只占 93 条；BlueVision 名下共
  395 个账户、约 22 万条素材，`ad_funded_by` 基本为空、`topic` 也分不出 imoo——只能按域名。
- **imoo.me** → 49 条素材，全在 `MOBICOLOR CO., LIMITED`（`AR16701205352422572033`）下。
- 首跑入库 141 条（1 条最后展示早于回溯窗口），2026-03～09 估算展示约 16–29 万次/月。

2026-09-28 首次实跑的结论（已写进 CSV）：

- **HUTT 在 Google 上按名检索不可用**：搜到 14 个广告主，全是 Enid Hutt Gallery、牙科诊所、
  T 恤店之类的同名公司，首跑入库的 9 条全是误报，已用 `*` 禁用。唯一可能相关的是
  `HUTT Ltd`（AR16809284089847742465），确认是自家后以 `true` 登记即可。
- **ECOVACS 的 Google 广告主是 `ECOVACS EUROPE GMBH`**（两个 id，已登记白名单）。
  数据集里只有 4 条视频素材，**全部只投法国**，德国为 0——Google 侧德国空是真实情况，
  不是漏采。ECOVACS 的德国投放主要看 Meta。
- 真实表里投放面字段叫 `surface`，不是早期资料里的 `surface_code`；首末展示日是 STRING。
- 每次查询实际预估扫描约 70–85 GB，周频约 350 GB/月，在 1 TiB 免费额度内。
  改成日频约 2.3 TB/月，会超出免费额度。

### 为什么这样设计

- **状态型 upsert，不是 append-only。** 两个源给的都是广告**生命周期累计值**，今天的值
  严格包含昨天的；日快照只会堆出单调递增的重复行。唯一键 `(source_code, ad_id)`，
  除 `first_seen_at` 外每次刷新。
- **exposure 带单位，不做成可加总的数。** Meta 是覆盖**人数**，Google 是展示**次数**，
  量纲不同，同一个数值列会诱导加总或比大小。
- **Google 「在投」是推断的。** 数据集只有最后展示日，最近 `CI_GOOGLE_ADS_ACTIVE_DAYS`（默认 7）
  天内还在展示即标 running。
- **周频。** `creative_stats` 约 150 GB，BigQuery 按扫描量计费：周频约 600 GB/月，在免费额度内。
  每次真查询前先 dryRun（免费）预估扫描量，超过 `CI_GOOGLE_ADS_MAX_GB`（默认 200）
  就跳过并告警 `cost_guard`，绝不让一条改坏的 SQL 悄悄烧钱。
- **Google 表结构运行时读取，不写死。** 公开资料里字段名互相矛盾（`creative_id` / `ad_id`），
  Google 也可能改表。每次先读真实 schema、按候选名解析列；缺必需列告警 `schema_changed`
  并跳过，不猜。可选列（如投放面）缺失只在日志里提示，对应字段留空。
- **不存 MinIO 快照，绝不存带 token 的 URL。** Meta 的 `ad_snapshot_url` 与分页 `paging.next`
  都内嵌 access_token：前者根本不请求，改存公开的 `facebook.com/ads/library/?id=`；
  后者只在内存里跟随。两个源都能按 id 随时重取，不需要回溯重解析。
- **Meta 文案会挂型号。** 文案过 `match_products_all()`，命中的型号进 `products` 列；
  ECOVACS 的扫地机广告也会出现（品牌级检索），没挂型号的就是非擦窗机品类。

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
core.ci_product        产品主数据（自家 is_own=true + 竞品同表），CSV 即权威；
                       kind=model 具体型号 / kind=series 媒体层系列兜底（见「系列兜底」）
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
| `social_media` | 社交媒体内容 | **按源判，排最前**：`youtube` / `instagram`（「im Test」视频也归这档） |
| `test` | 实测评测 | 标题/正文含 `im Test` / `Testbericht` / `getestet` / `Praxistest` / `ausprobiert` / hands-on |
| `promo` | 促销 | price 层（mydealz 本质是 deal 站），或含 `Bestpreis` / `Angebot` / `Rabatt` / 数字+€ / 折扣百分比 |
| `media_review` | 媒体报道但非实测 | media 层且非上述两类 —— 上市消息、发布会、导购、获奖 |
| `discussion` | 用户讨论 | 其余 social 层（Reddit、mydealz 讨论）+ retail 层（Amazon 评论是用户内容） |
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

每条情报线一份（`scope` = `hutt` / `imoo`；分线前的旧记录是 `all`）：

```sql
SELECT digest_on, scope, mention_cnt, source_codes, summary FROM mart.ci_digest
ORDER BY digest_on DESC, scope LIMIT 2;
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
| `ci-ads` | 每周一 06:30 | `CI_ADS_CRON` |

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

### 把本地积累的历史推到云端

价格 delta 序列只能靠日频采样攒（见上）。本地机器往往比服务器更早开跑，
或者服务器一直在 `CI_DRY_RUN=true` 干跑——这时本地 `raw.ci_*` 里的历史比云端完整，
可以用 `scripts/ci_sync_data.sh` 一次性推上去，云端曲线立即补齐：

```bash
# 在本地执行；一条管道直达，不落临时文件
bash scripts/ci_sync_data.sh export \
  | ssh deploy@<服务器> 'cd /opt/channelhub && bash scripts/ci_sync_data.sh import -'
```

每张表回显 `received / inserted` 两列：`inserted` 是真正新增，差额即云端已有的行。
幂等要点：

- 搬 `ci_offer / ci_listing_stat / ci_mention / ci_unmatched` 四张事实表，
  按各表 UNIQUE 约束 `ON CONFLICT DO NOTHING`——只补缺，**绝不覆盖**云端已有行，
  重复跑 `inserted` 全为 0。整份导入在一个事务里，任一表失败整笔回滚。
- `product_id` 是两边一致的文本短码（`core.ci_product` seed），无需重映射；
  自增主键与 `total_cents` 生成列由目标端自己算。
- `snapshot_id` 置 NULL：它指向源端 `raw.ci_snapshot` 的自增 id，云端对不上。
  所有 `mart.v_ci_*` 视图都不用它，只影响「回溯重解析」——那需要连 MinIO
  `ci-archive` 桶一起 `mc mirror`，不在脚本范围。
- 不搬 `raw.ci_snapshot`、`core.ci_hashtag`（Instagram 配额账本按账号计，不能混）
  和 `mart.ci_digest`。

推完之后云端照常按排期采集，两边自然汇合；反方向（云端 → 本地）同样适用。

### 告警

复用 `raw.ingest_alert` 去重表，主题前缀 `[ChannelHub]`，同一「源 × 原因」当天只发一次。

| reason | 含义 | 处理 |
|---|---|---|
| `bot_wall` | 命中验证码/403/429 | 换 `curl_cffi` 指纹或上代理；Amazon 可先 `CI_AMAZON_ENABLED=false` |
| `parse_empty` | 页面取到但解析全空 | 多半改版：从 MinIO 取当天快照，照着修解析器 |
| `zero_matched` | 有候选但一条都没匹配上 | 消歧正则或产品主数据出问题，先跑 `check_ci_matching.py` |
| `api_auth_required` | 接口要求鉴权/签名（如 mydealz 401） | 该源不可用；**0 条结果不代表市场上没有** |
| `collect_failed` | 该源整体异常 | 看 Prefect UI 日志 |
| `schema_changed` | Google `creative_stats` 缺必需列（广告层） | Google 改表了，按告警里的列名更新 `flows/ci_ads.py` 的候选名 |
| `cost_guard` | BigQuery 预估扫描超上限（广告层） | 先确认 SQL 没改坏；确需更多再调 `CI_GOOGLE_ADS_MAX_GB` |
| `domain_lookup_failed` | 透明度中心按域名检索失败（广告层，`ci_ad_domain`） | 本轮已沿用旧素材清单、未删数据；多半是网页接口改版，按告警里的报错修 `flows/ci_ads.py:parse_tc_page` / `tc_request` |

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

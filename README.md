# ChannelHub — 进销存数据平台（本地开发）

本地开发阶段可运行的进销存数据平台基础设施。当前为**基础设施轮次**：只搭好
数据库、对象存储与配套工具的编排，**暂不含业务表 DDL**。后续轮次再做表结构、
Python 邮件 ETL、Prefect flow 与 Metabase 报表。

## 服务一览

| 服务 | 镜像 | 访问地址 | 用途 |
|---|---|---|---|
| PostgreSQL | `postgres:16` | `127.0.0.1:5432`(仅本机) | 主业务库 `channelhub` + `metabase` / `prefect` / `superset` 支撑库(同一实例) |
| pgAdmin | `dpage/pgadmin4:latest` | `127.0.0.1:5050`(仅本机) | 数据库可视化管理(已预注册服务器 ChannelHub-PG) |
| MinIO | `minio/minio:latest` | `127.0.0.1:9000/9001`(仅本机) | 邮件源文件备份对象存储,桶 `email-archive` |
| **Superset** | `apache/superset:4.1.1` | **`https://<PREFECT_PROXY_HOST>`(公网,主 BI)** | 主对外 BI 报表 — Caddy 反代 + 自签 HTTPS |
| Metabase | `metabase/metabase:latest` | `127.0.0.1:3000`(仅本机) | 备用 BI;走 SSH 隧道访问 |
| Prefect 3 OSS | `prefecthq/prefect:3-latest` | `127.0.0.1:4200`(仅本机) | 任务编排 server;走 SSH 隧道访问(OSS 无认证不可公网暴露) |
| Caddy | `caddy:2` | `0.0.0.0:443` | HTTPS 反代到 Superset;证书 SAN 写 `PREFECT_PROXY_HOST` |

> 镜像统一用滚动 tag 以保证可拉取；如需可复现环境，请在 `docker-compose.yml`
> 中固定为具体版本 tag。

## 前置条件

- 已安装 **Docker Engine + Docker Compose 插件**（`docker compose version` 可用）。
  > 本机当前未安装 Docker，需先安装：参见
  > https://docs.docker.com/engine/install/ ，并将当前用户加入 `docker` 组。
- 端口 `5432 / 5050 / 9000 / 9001 / 3000 / 4200 / 443` 未被占用（如冲突见下文「故障排查」）。
  > 443 给 Caddy（Prefect HTTPS 反代）；若被占用改 `.env` 的 `PREFECT_HTTPS_PORT`。

## 快速开始

```bash
# 1. 准备环境变量（.env 已含 openssl 随机生成的密码；如需重置可参考模板）
cp .env.example .env   # 已有 .env 则跳过；重置密码：openssl rand -hex 24

# 2. 一键拉起全部服务
docker compose up -d

# 3. 查看状态（postgres 应为 healthy，minio-init 跑完即 exited 0）
docker compose ps

# 4. 初始化数据层 + Metabase（幂等，第一次必跑，之后改了迁移/seed 也可再跑）
bash scripts/initialize.sh
```

首启 PostgreSQL 时会自动执行 `db/init/00_create_support_databases.sql`，
创建 `metabase`、`prefect` 两个**空支撑库**（不含业务表）。业务表/视图/物化层、
BI 只读角色 `bi_readonly`、GTIN 白名单 seed、Metabase 管理员 + 数据源 + 仪表盘，
全部由 `scripts/initialize.sh` 串起来跑一次到位。

### `scripts/initialize.sh` 做了什么

依次跑六步,任意一步失败即停下(全部幂等,可重复跑):

1. 按编号顺序应用 `db/migrations/*.sql` — 建 `raw / core / mart` schema 与对象
   - 注:这些迁移设计为**前向应用一次**(002/003/005 互相重塑同一组视图)。
     脚本检测到 `core.gtin_whitelist` 存在就跳过本步,认为数据层已初始化。
     新增迁移(如未来的 007)请手动 `psql -f db/migrations/007_*.sql` 应用。
2. 用 `.env` 里 `BI_READONLY_PASSWORD` 设 `bi_readonly` 角色密码
3. 装载 `db/seed/gtin_whitelist.csv` 到 `core.gtin_whitelist`
4. 在 Postgres 建空的 `superset` 元数据库(若不存在);superset-init 容器会在里面建 schema
5. 调 `scripts/metabase_setup.py` — 建 Metabase 管理员、接 channelhub 数据源、
   建「ChannelHub Overview」仪表盘(走 SSH 隧道使用)
6. 调 `scripts/superset_setup.py` — Superset 用 admin 凭据注册 ChannelHub 数据源

### 已经手动 setup 过 Metabase,密码对不上?

`scripts/metabase_setup.py` 走的是登录 → 操作 API。如果你之前在浏览器手动跑过
Metabase 首次 setup 向导，那个时候输的管理员密码会落进 `metabase` 元数据库；
跟 `.env` 里 `MB_ADMIN_PASSWORD` 不一致就会 401。两种修法:

- 改 `.env` 的 `MB_ADMIN_EMAIL/PASSWORD` 跟当时你手动输的对齐
- 或清空 Metabase 元数据库重来(数据层 metabase 库,**不**含业务数据):
  ```bash
  docker compose stop metabase
  docker compose exec -T postgres psql -U channelhub -d postgres \
    -c "DROP DATABASE metabase;" -c "CREATE DATABASE metabase;"
  docker compose up -d metabase && sleep 30
  bash scripts/initialize.sh
  ```

## 凭据在哪看

所有密码由 `openssl rand -hex 24` 随机生成，保存在项目根目录 **`.env`**
（权限 600，已被 `.gitignore` 排除，不会进版本库）：

- PostgreSQL：`POSTGRES_USER` / `POSTGRES_PASSWORD` / 库 `POSTGRES_DB`
- pgAdmin 登录：`PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD`
  - 登录后左侧已自动出现 **ChannelHub-PG**；首次展开时输入一次
    `POSTGRES_PASSWORD` 即可（出于安全，密码不预存在 servers.json）。
- MinIO 控制台：`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
- Metabase / Prefect：复用上面的 PostgreSQL 账号连接各自支撑库，无需单独凭据。

查看（不打印明文，仅看键）：`grep -oE '^[A-Z_]+=' .env`

## 常用命令

```bash
docker compose up -d                 # 启动/更新
docker compose ps                    # 查看状态
docker compose logs -f <service>     # 跟踪日志，如 logs -f prefect
docker compose exec postgres psql -U channelhub -d channelhub   # 进入 psql
docker compose exec postgres psql -U channelhub -c '\l'         # 列出三个库
docker compose stop                  # 停止（保留数据卷）
docker compose down                  # 停止并移除容器（数据卷保留）
docker compose down -v               # 连同数据卷一起删除（谨慎，会清空数据）
```

数据持久化于命名卷：`channelhub_pg_data`、`channelhub_pgadmin_data`、
`channelhub_minio_data`、`channelhub_prefect_data`。`down`（不带 `-v`）不会丢数据。

## 故障排查

- **端口冲突**：编辑 `.env` 中对应 `*_PORT` 改成空闲端口，再 `docker compose up -d`。
- **改了 init SQL 但没生效**：`db/init/*.sql` 只在 **postgres 数据卷为空的首次启动**
  执行。已初始化后需要重跑：`docker compose down -v` 重建卷（会清空数据），
  或手动 `docker compose exec postgres psql ...` 执行。
- **业务表在哪**：本轮不创建。九类业务对象（产品/颜色/SKU/渠道/门店/库存余额/
  进销存流水/邮件导入记录/去重记录）留待后续 DDL 轮次。
- **pgAdmin 没看到预注册服务器**：servers.json 仅在 `pgadmin_data` 卷为空时导入，
  重置：`docker compose down && docker volume rm channelhub_pgadmin_data`。
- **pgAdmin 启动崩溃 / 无限重启**：新版 pgAdmin 拒绝 `.local` 等保留域名邮箱，
  `PGADMIN_DEFAULT_EMAIL` 必须用正常域名（如 `admin@channelhub.com`）。
- **远程机器开 Prefect UI 整页白屏（控制台 `crypto.randomUUID is not a function`）**：
  浏览器规定 `crypto.randomUUID()` 只在 **secure context**（https，或
  localhost/127.0.0.1）可用。本机 `http://localhost:4200` 正常，但**别的机器经
  明文 `http://<LAN-IP>:4200` 访问不是 secure context**，新版 UI 的 csrf 代码
  调 `randomUUID` 抛错 → 白屏。解决：远程一律走 Caddy 的 HTTPS，即
  **`https://<PREFECT_PROXY_HOST>`**（默认 https://192.168.178.73）。
  - 自签证书，浏览器首次会提示不安全 → 「高级 / 继续前往」即可。
  - 改访问 IP/域名：改 `.env` 的 `PREFECT_PROXY_HOST` 后 `docker compose up -d caddy`。
  - 想换掉自签证书警告：用内网域名 + 受信 CA，或导入 Caddy 的根 CA
    （`docker compose cp caddy:/data/caddy/pki/authorities/local/root.crt ./`）。
- **Prefect 页面报 `Oops. Something went wrong.`**：UI 是 SPA，需用浏览器可达的
  API 地址。compose 已设 `PREFECT_UI_API_URL=/api`（相对路径），无论经 localhost
  还是局域网 IP 访问都能连通。仍报错时**强制刷新浏览器清缓存**（Ctrl+Shift+R），
  旧版前端会缓存错误的 API 地址。

## 邮箱备份 flow（IONOS IMAP → MinIO，定时）

把 IONOS 邮箱（默认仅 `INBOX`）的**原始邮件源文件**定时备份到 MinIO
`email-archive` 桶，做持久化。**只读连接、BODY.PEEK 抓取，绝不修改/删除/标记
已读**原邮件；按 `(folder, UIDVALIDITY, UID)` 生成确定性对象键，已存在即跳过 ——
幂等、可断点续跑。每封邮件存两个对象：

- `email/<邮箱>/<folder>/<uidvalidity>/<uid>.eml` —— 原始 RFC822
- 同名 `.json` —— 元数据（subject/from/to/date/message-id/sha256 等）

相关文件：[flows/email_backup.py](flows/email_backup.py)、[prefect.yaml](prefect.yaml)、
[flows/Dockerfile](flows/Dockerfile)。编排：`prefect-deploy`(一次性，建 work pool +
注册 deployment) → `prefect-worker`(常驻，跑 flow run)。

### 启用步骤

1. 在 `.env` 填两项（**之前留空，现在填**）：
   ```
   EMAIL_USER=你的完整邮箱地址          # 例 name@yourdomain.com
   EMAIL_PASSWORD=该邮箱密码            # 不是 IONOS 账户登录密码
   ```
   其余 `EMAIL_*` 已给好默认（`imap.ionos.de:993` SSL、仅 INBOX、每 5 分钟）。
   SMTP 也已写入 `.env`，供解析告警与邮件服务回信用。
2. 起服务（首次会 build worker 镜像）：
   ```bash
   docker compose up -d --build
   ```
3. 在 Prefect UI（http://localhost:4200）→ Deployments 可见 **email-backup**，
   已带 `*/5` cron；点 **Run** 可立即手动触发一次，运行历史在 UI 可查。

### 为什么备份能跑每 5 分钟

本 flow 自己很轻（实测**约 3 秒**：一次 IMAP SEARCH + 每封一次 MinIO `stat_object`）。
真正重的是下游 `parse-sell-through`（实测**平均 136 秒、最长 220 秒**，因为它每次
全量重扫归档里所有 `.eml`）。所以 email-backup **只在真的收到新邮件时**才
`run_deployment` 触发下游（`if total["uploaded"]`）—— 没有这道闸，cron 一提速就会让
解析 flow 几乎连续占用 worker，并发的 `mart.refresh_all()` 还会 TRUNCATE 掉 Superset
正在读的 mart 表。

代价是丢了「每小时无条件重跑」的隐式自愈，所以 `parse-sell-through` 配了一条
**每日兜底 cron**（`.env` 的 `EMAIL_PARSE_CRON`，默认 `0 3 * * *`）补位。
**别把 `EMAIL_PARSE_CRON` 调回高频。**

### 改频率 / 文件夹

改 `.env` 的 `EMAIL_BACKUP_CRON`（UTC cron）或 `EMAIL_FOLDERS`（逗号分隔），
然后重新注册：`docker compose up -d --build --force-recreate prefect-deploy prefect-worker`。

## 数据分层与迁移

ELT 分层（medallion）：

| 层 | schema | 说明 |
|---|---|---|
| RAW | `raw` | 供应商文件**忠实落地**，全 TEXT 不转换，带完整血缘（源邮件/MinIO 对象/行号），可追溯；**不去重**（同文件多发都留，全审计） |
| CORE | `core` ✅ | 规范化 + **去重** + **GTIN 白名单过滤**（视图实现，实时）：德式日期→date、量→int、ISO 周；冲突按既定优先级解析；只放行自家 GTIN |
| MART | `mart` ✅ | 从 core（白名单后）按 **GTIN 粒度**物化为**真实表**（`dim_company`/`dim_store`/`dim_product`/`fact_*`），由 parse flow 末尾 `mart.refresh_all()` 单事务 TRUNCATE+INSERT 重建；Metabase 读此层 |

**冲突解析优先级**（[db/migrations/003_core_conflict_policy.sql](db/migrations/003_core_conflict_policy.sql)，取代 002 视图定义）：
同一文件发多次 → raw 忠实保留每份（可追溯收到几次）；core 解析冲突的规则：

1. **先看 `transaction_date`：最新日期胜出**
2. **同一 `transaction_date`：以最新发来的报表为准** —— 用 IMAP 收件顺序判定
   （从 `source_object_key` 的 `.../<UIDVALIDITY>/<UID>.eml` 提取数字排序），
   确定性强、**与入库先后无关**；最终兜底 `ingested_at`/`raw_id`

两层产出（core 全部视图，实时一致、零编排，量大再物化）：

- `core.fact_sell_through` —— **历史事实**，每 供应商×周期×日期×门店×SKU 一行
  （同期重发/更正取最新报表）→ 趋势报表
- `core.fact_sell_through_current` —— **当前库存快照**，每 门店×SKU 取最新
  `transaction_date`（同期取最新报表）→ “当前库存以什么为准”的答案
- 视图链：`v_sell_through_union`（多供应商扩展点）→ `v_sell_through_keyed`
  （解析日期/收件序）→ **`v_sell_through_whitelisted`（GTIN 白名单卡点）** →
  `v_sell_through_dedup` → `dim_*` / 两个 fact（带血缘回溯 raw）

迁移脚本放 [db/migrations/](db/migrations/)，文件名带序号；幂等可重复执行。
应用方式（postgres 卷已初始化，`db/init` 不会再跑，故手动执行）：

```bash
# 按序应用全部迁移（幂等，可重复执行；003 取代 002 视图链，005 又取代 003 视图链；
# 006 在 core 之上新建 mart 物化层，不取代视图链；007 在 mart 上加 PSI 口径视图 v_psi；
# 008 加 PLZ→Bundesland 参照 + 按州视图 v_psi_bundesland；
# 009 加 Hutt 网店订单表 + BI 口径视图；010 加邮件服务记账表 raw.mail_request）
for f in db/migrations/0*.sql; do
  docker compose exec -T postgres psql -U channelhub -d channelhub \
    -v ON_ERROR_STOP=1 -f - < "$f"
done
```

### GTIN 白名单（只放行自家产品）

邮箱报表里混有大量**别人的产品**。按分层约定 raw **忠实保留全部**（审计不动），
过滤放在 core 的**单一卡点** `core.v_sell_through_whitelisted`：只有 GTIN（归一后）
命中 `core.gtin_whitelist` 的行才进入 `dedup → dim/fact/current`，BI 自动只看到自家
产品。删/加一个 GTIN 即从 BI 移除/恢复某产品，**无需回灌 raw 或重跑 ETL**（视图实时）。

- 迁移 [db/migrations/005_core_gtin_whitelist.sql](db/migrations/005_core_gtin_whitelist.sql)
  （**取代 003 视图链**并注入白名单 JOIN）：建 `core.gtin_whitelist`、归一函数
  `core.norm_gtin`（去首尾空白/Excel `.0`/空格连字符）、同步函数
  `core.sync_gtin_whitelist()`、卡点视图，及可见性视图 `core.v_gtin_unmatched`。
- 白名单**权威源是 CSV**：[db/seed/gtin_whitelist.csv](db/seed/gtin_whitelist.csv)
  （`gtin,note` 表头，每行一个 GTIN，`note` 可填产品名/负责人，可留空）。版本化、
  可审阅。编辑后跑装载器同步（CSV 没有的会被删除，其余 upsert）：
  ```bash
  # 1) 编辑 db/seed/gtin_whitelist.csv 填入你的 GTIN
  # 2) 应用迁移（见上方循环，或单独应用 005）
  # 3) 同步白名单（幂等；空白名单默认中止以防把 core/BI 挡空）
  bash db/seed/load_gtin_whitelist.sh
  ```
- ⚠️ **次序**：白名单为空时 core 会**全空**（一切被挡）。先填 CSV 再跑装载器；
  装载器在 0 条时会**中止**（确需清空加 `--allow-empty`）。
- 排查是否漏配自家产品（被挡在外的 GTIN + 名称/行数/最近出现）：
  ```sql
  SELECT * FROM core.v_gtin_unmatched ORDER BY raw_rows DESC;
  ```

### MART 物化层（BI 口径，真实表 + 链式刷新）

数据量上来后，BI 不宜每次直查 core 视图链（每查重算 union→keyed→whitelist→dedup）。
新增 `mart` schema 把 BI 口径物化为**真实表**，由解析 ETL 末尾链式刷新：

- 迁移 [db/migrations/006_mart_materialized.sql](db/migrations/006_mart_materialized.sql)
  （**在 core 之上新建，不取代 core 视图链**——与 003/005 不同；core 仍是实时
  事实源 / 血缘 / GTIN 白名单单一卡点，mart 只是其周期性快照）：建
  `mart.dim_company` / `mart.dim_store` / `mart.dim_product` /
  `mart.fact_sell_through` / `mart.fact_sell_through_current` + 刷新函数
  `mart.refresh_all()`。
- **键**：门店 = 渠道既定 `(supplier_code, store_id)`；产品 = **归一 GTIN**
  `gtin_norm`（`core.norm_gtin`，全局一行）；新增**运营公司维**
  `dim_company (supplier_code, company)`（`company` 为空→`(unknown)`），
  门店 / 事实回挂公司。
- **粒度变更（重要）**：mart 事实按 **供应商×周期×日期×门店×GTIN** 去重
  （core 仍按 `customer_sku_code`，实时不变）。同店同周同 GTIN 若有多个
  `customer_sku_code`，按既定冲突优先级（最新发来报表）**取一行**——按 GTIN
  折叠，**不**跨 SKU 码求和（如需合量，把 `refresh_all()` 改为 SUM 聚合）。
- **刷新**：`mart.refresh_all()` 读 `core.v_sell_through_whitelisted`（白名单后）
  建临时表 → 单事务 `TRUNCATE`+`INSERT` 重建全部 mart 表（读者只见旧或新，
  无半态），返回各表行数。**链在 `parse-sell-through` flow 末尾**自动调用
  （解析入库后立即刷新，与解析同一次运行；无需新增 deployment/定时；即便部分
  `.eml` 失败，已入库 raw 也会刷新，刷新失败则整体 flow 失败）。
- `bi_readonly` 已对 `mart` 加只读 + 默认权限；Metabase 改读 `mart.*`。
- 手动刷新 / 排查：
  ```sql
  SELECT * FROM mart.refresh_all();   -- 返回 公司/门店/产品/历史/当前 行数
  ```

**每供应商一张独立 raw 表**（约定 `raw.sell_through_<供应商>`）：各大渠道 Excel
列结构不同，每表精确镜像该供应商文件，最忠实可追溯；ETL 按 Excel 表头签名识别
供应商并路由。已建
[db/migrations/001_raw_sell_through_expert.sql](db/migrations/001_raw_sell_through_expert.sql)：
`raw.sell_through_expert`（Expert 15 列全 TEXT + 血缘列 + 生成列 `row_hash` +
物理源行幂等唯一约束）与 `raw.ingest_alert`（未识别附件告警去重）。

## 解析 ETL flow（MinIO .eml → raw.sell_through_*）

[flows/parse_sell_through.py](flows/parse_sell_through.py)：扫 MinIO `email-archive`
所有 `.eml`，按附件扩展名分发到**两条互不纠缠的解析路径**（文件内按
「共用 / Expert / Hutt / 编排」四段组织）：

- `.xlsx` → **Expert 渠道周报**：`openpyxl` 读表头与 `XLSX_REGISTRY` 表头签名
  比对，命中 → 逐行带血缘 `INSERT ... ON CONFLICT DO NOTHING` 进
  `raw.sell_through_expert`（幂等，取值原样 TEXT，不规范化）
- `.csv` → **Hutt 网店订单**（Shopify `orders_export_*.csv`）：表头签名命中
  核心列集（`HUTT_REQUIRED`）→ 同样带血缘幂等入 `raw.sell_through_hutt_shop_de`
  （79 列映射自动从列名推导，血缘约定 `source_sheet`=文件名、数据行号从 2 起）
- 任何签名都不命中 → 经 SMTP（`smtp.ionos.de:465` SSL）给 `ALERT_EMAIL_TO`
  （收件人地址配置在 `.env`，不入库）发告警邮件；`raw.ingest_alert` 去重，
  同一未识别文件只告警一次
- 新增 xlsx 供应商 = `XLSX_REGISTRY` 加一条表头签名 + 一个 `raw.sell_through_<x>` 表

另有一道**自发信护栏**：`From` 是本系统自己的邮箱、或带 `X-ChannelHub-Rule` 头的
邮件直接跳过不解析 —— 邮件服务 flow 的回信附件（`ai-sunrise-*.csv`）表头不匹配
`HUTT_REQUIRED`，万一那封回信被抄送/转发落回 INBOX，没这道闸就会收到一封
「未识别 csv 表头」的误告警。

编排：Prefect deployment **parse-sell-through**，由 **email-backup 收到新邮件后
链式触发**（`run_deployment`，timeout=0 非阻塞），保证「备份完立刻解析、解析完
立刻刷新 mart」一条链；另配 `EMAIL_PARSE_CRON`（默认每日 03:00 UTC）兜底补跑。
改 `.env` 的 cron 后重新注册（`docker compose up -d --build --force-recreate prefect-deploy prefect-worker`）。

## 邮件服务 flow（收信 → 处理 → 回信）

[flows/mail_service.py](flows/mail_service.py)：和解析 ETL 读同一批 MinIO `.eml`，
但干的是**请求-应答**的活 —— 读懂来信、生成文件、**回信给发件人**。

之所以单独一个 flow 而不是塞进 `parse_sell_through`：那条是「把附件吞进 `raw.*`」的
单向 ETL，这条要对外发信，失败语义完全不同（发信必须 at-most-once，宁可漏发也不能
重复轰炸对方），混在一起会让回信失败把整条 ETL 标红。

### 当前规则：ai-sunrise 订单导出

发件人以 `@ai-sunrise.de` 结尾、附件是 Shopify `orders_export.csv` → 生成物流用的
`ai-sunrise-DDMMYYYY.csv` 作附件回信。列映射：

| 输出列 | 源列 | 说明 |
|---|---|---|
| `Referenznummer` | `Name` | `#1547` → `ais_1547` |
| `Bestelldatum` | `Created at` | → `24.08.26`（DD.MM.YY） |
| `Stück` / `Produktname` / `SKU` | `Lineitem quantity` / `name` / `sku` | 原样 |
| `Name` | `Shipping Name` | 订单级 |
| `Straße` | `Shipping Address1` + `Address2` | 用 `", "` 拼，只在两边都非空时加分隔符 |
| `PLZ` | `Shipping Zip` | 去掉 Excel 防丢零的撇号；**不补零**（AT 是 4 位） |
| `Ort` / `Land` | `Shipping City` / `Country` | 订单级 |
| `Zusatz` | `Shipping Company` | 订单级 |

**订单级字段要前向填充**：Shopify 导出里一个订单的地址/日期只出现在该订单的
**第一行**，后续行项目那些列全是空的；而输出要求每个行项目都带完整地址。所以走两遍
（先按订单号收集每个字段的第一个非空值，再按原始行序输出）。
输出是**分号分隔 + UTF-8 BOM + CRLF**，德语 Excel 双击即正确打开（BOM 不能省）。
不做业务过滤，取消单/未付款单照样输出（1:1 转换）。

### 两道护栏（**必须先于规则匹配**）

1. **不回自己的信** —— `From` == `EMAIL_USER`/`SMTP_USER` 直接跳过。
   这条不是可选项：规则匹配的是「发件人以 ai-sunrise.de 结尾」，而
   `data@ai-sunrise.de` 自己也满足，没这道闸就是自触发死循环。
2. **不回自动信** —— `Auto-Submitted` / `Precedence: bulk` / `List-Id` /
   我们发信时打的 `X-ChannelHub-Rule` 头，命中任一即跳过（防和对方的自动回复对打）。

回信自带 `Auto-Submitted: auto-replied` + `X-ChannelHub-Rule` + `In-Reply-To`
（挂在原话题下），所以即便回信被转发回 INBOX 也会被两个 flow 一起跳过。

### 幂等：绝不重复发信

记账表 `raw.mail_request`（见 [010 迁移](db/migrations/010_raw_mail_request.sql)），
**先占坑再发信**：`INSERT ... ON CONFLICT DO NOTHING`，`rowcount=0` 就说明这封处理过，
直接跳过、连 `.eml` 都不下载。中途失败的行会**留在 `processing` 且不自动重试**
（重试可能变成重复发信），同时给 `ALERT_EMAIL_TO` 发告警。

排查/重发：

```bash
docker compose exec -T postgres psql -U channelhub -d channelhub -c \
  "SELECT rule_key, status, email_from, reply_file_name, rows_out, claimed_at
     FROM raw.mail_request ORDER BY request_id DESC LIMIT 20;"

# 确认无误后想让某封重发：删掉那行，下次 mail-service 运行会重新处理
docker compose exec -T postgres psql -U channelhub -d channelhub -c \
  "DELETE FROM raw.mail_request WHERE source_object_key='email/…/42.eml';"
```

### DRY RUN 开关

`.env` 的 `MAIL_SERVICE_DRY_RUN`（**默认 `true`**）：为 true 时照常生成附件、照常
记账，但**不实际发信**，只把收件人/附件名/大小打进日志。首次上线务必先 true 跑通，
确认无误再改 `false`。

### 加新的邮件处理功能

往 `flows/mail_service.py` 的 `RULES` 加一条 + 写一个 handler，flow 骨架不用动：

```python
RULES = [
    {
        "key": "你的规则名",              # 也是 raw.mail_request 的去重键之一
        "from_domain": "example.de",      # 发件人地址后缀
        "match_attachment": 你的识别函数,   # (fn, payload) -> bool
        "handler": 你的处理函数,            # -> (附件名, 附件字节, 回信正文, 行数)
        "reject_body": "识别不了时的回信正文",
    },
]
```

### 离线自检

改了转换逻辑或护栏后先跑这两个（不碰邮箱、不碰生产数据）：

```bash
docker run --rm -v "$PWD":/w -w /w channelhub-prefect-worker \
  python scripts/check_ai_sunrise_transform.py   # 转换结果逐格比对 fixture
docker run --rm -v "$PWD":/w -w /w channelhub-prefect-worker \
  python scripts/check_mail_guards.py            # 两道护栏 + 回信自环
```

样例数据在 [tests/fixtures/](tests/fixtures/)（`orders_export.csv` 输入、
`ai-sunrise-expected.csv` 期望输出）。

## 可视化（Superset 主对外 + Metabase 备用 + 只读角色）

- **只读角色** `bi_readonly`（[db/migrations/004_bi_readonly_role.sql](db/migrations/004_bi_readonly_role.sql)）：
  仅 `SELECT` raw+core（006 起含 mart），无写权限。Superset / Metabase / 日后 LLM 都用它连库,权限隔离。
  密码不入迁移文件,应用后单独设(`scripts/initialize.sh` 第 2 步会自动做):
  ```bash
  docker compose exec -T postgres psql -U channelhub -d channelhub \
    -v ON_ERROR_STOP=1 -v pw="$BI_READONLY_PASSWORD" <<<"ALTER ROLE bi_readonly PASSWORD :'pw';"
  ```

### Superset(主对外 BI,公网 443)

- 镜像 `apache/superset:4.1.1`,经 Caddy 反代 + 自签 HTTPS 暴露到 443。配置见
  [superset/superset_config.py](superset/superset_config.py)(元数据库走同 Postgres 的
  `superset` 库,启 ProxyFix 信任 Caddy 转发头)。
- 一次性 init 容器 `superset-init` 跑 `superset db upgrade` + `create-admin`(凭据见
  `.env` 的 `SUPERSET_ADMIN_USERNAME/PASSWORD`)+ `superset init`(加载默认角色权限)。
- 注册数据源由 [scripts/superset_setup.py](scripts/superset_setup.py) 完成:用 admin 凭据
  登录 → POST `/api/v1/database/` 把 `bi_readonly@postgres/channelhub` 加为 "ChannelHub" 数据源。幂等。
- 不预建大部分图表与仪表盘 — 主要用 Superset 在于丰富可视化(deck.gl 地图热密度、Sankey、
  Treemap 等),按需在 UI 或 API 增量建。PSI 基础看板是例外,已脚本化(见下)。
- 访问: `https://<服务器IP>` → 浏览器警告自签证书 → 高级 → 继续 → admin 凭据登录

### PSI 看板(进销存:Purchase / Sale / Inventory)

渠道报表只给**销售 S(门店售出)**和**库存 I(期末在手)**,**没有采购 P**。
P 由库存恒等式从相邻两期推出:

```
期末库存 = 期初库存 + 采购 − 销售   ⇒   P = I(本期) − I(上期) + S(本期)
```

“上期”取同一 (供应商, 门店, GTIN) 上一条有数据的 `transaction_date`(`LAG` 按日期,
**不假设周连续** —— 报表存在缺周);某店某品的**首期**无上期 → P=NULL(不臆造)。

- **口径视图** [db/migrations/007_mart_psi.sql](db/migrations/007_mart_psi.sql) → `mart.v_psi`:
  读 `mart.fact_sell_through`(已白名单+按 GTIN 去重),随 `mart.refresh_all()` 自动最新。
  含 `purchase_qty / sale_qty / inventory_qty`,并带 `is_latest`(每店每品最新一期)——
  **库存是存量,跨周不可加**,产品/门店维“当前库存”务必用 `is_latest = true` 过滤。
  另含 `sku`(SKU 编码 + 品名)和 `sale_qty_last_4w`；DOS 使用
  `SUM(当前库存) × 28 / SUM(最近4周销量)`，分母为 0 时为空。
  ```bash
  docker compose exec -T postgres psql -U channelhub -d channelhub \
    -v ON_ERROR_STOP=1 -f - < db/migrations/007_mart_psi.sql
  ```
- **看板搭建** [scripts/superset_expert_dashboard.py](scripts/superset_expert_dashboard.py)(幂等,
  存在则更新):注册 `mart.v_psi` 数据集 → 建 7 张图(Sale/Inventory 周趋势 / 各产品销售 /
  当前库存按产品 / DOS KPI / SKU DOS 表 / SKU 销售明细 / Sale by Bundesland)→
  组装看板 `PSI 看板`(slug=`psi`)；顶部提供 **Company → SKU** 级联筛选：
  选 company、不选 SKU 即看该 company 全部 SKU，继续选 SKU 即看单品。先跑过
  `superset_setup.py`(需数据源 `ChannelHub`),再:
  ```bash
  docker run --rm --network channelhub_channelhub --env-file .env \
    -v "$PWD/scripts/superset_expert_dashboard.py:/dash.py:ro" \
    prefecthq/prefect:3-latest python /dash.py
  # 完成后访问 https://<服务器IP>/superset/dashboard/psi/
  ```
  > 若某张图首次打开提示无 query context,在 Explore 里点一次 **Run → Save** 即可重建
  > (脚本已写入 `query_context`,正常无需此步)。

### 按联邦州统计 SO(PLZ → Bundesland)

渠道报表只给门店 PLZ/城市,没有联邦州;德国 PLZ 区(Leitregion)与州界**不重合**
(同一 2 位前缀常跨 2~3 个州,如 `37` 横跨 Hessen/Thüringen/Niedersachsen/NRW),
不能按前缀粗判。故用 GeoNames 全量 PLZ→州参照(每 PLZ 取众数州):

- **参照数据** [db/seed/plz_bundesland.csv](db/seed/plz_bundesland.csv)(~10.8k 行,CSV 即权威),
  由 [db/seed/load_plz_bundesland.sh](db/seed/load_plz_bundesland.sh) 装入 `core.plz_bundesland`。
  来源 GeoNames `DE.zip`,用 `admin_code1`(数字/字母两套编码都归一)按 PLZ 取众数州 —— 边界
  门店(如 Bad Mergentheim `97…`→Baden-Württemberg)实测正确。
- **口径视图** [db/migrations/008_geo_plz_bundesland.sql](db/migrations/008_geo_plz_bundesland.sql)
  → `mart.v_psi_bundesland`(= `v_psi` + `bundesland`,门店 PLZ 连参照)。
  **SO(Sell-Out=售出量)按州** = `GROUP BY bundesland, SUM(sale_qty)`。
- 看板里对应「**Sale by Bundesland**」表(各州 Sale + 门店数),由看板脚本自动建。
- 新增门店若 PLZ 不在参照 → `bundesland=(unknown)`(不丢行);补 CSV 后重跑 loader 即可。

### Hutt Online Shop 看板(自有网店电商)

Hutt 自有 Shopify 网店的订单报表,数据源是邮件 ETL 落进 `raw.sell_through_hutt_shop_de`
的 Shopify 订单导出(全 text 列,1 行 = 1 订单行项;当前每单恰好 1 行项)。

- **口径视图** [db/migrations/009_mart_hutt_shop.sql](db/migrations/009_mart_hutt_shop.sql)
  → `mart.v_hutt_shop_orders`:类型化(金额 numeric、时间 timestamptz)+ 清洗
  (邮编去前导撇号、DE 补足 5 位)。核心口径 **`net_total` = `total` − `refunded_amount`**
  (净收入,退款即扣);`region` = DE 按 PLZ 映射联邦州(复用 008 的
  `core.plz_bundesland` 参照),其他国家给国家码。已加入 `IDEMPOTENT_MIGRATIONS`,
  deploy 自动重放;本地手动应用:
  ```bash
  docker compose exec -T postgres psql -U channelhub -d channelhub \
    -v ON_ERROR_STOP=1 -f - < db/migrations/009_mart_hutt_shop.sql
  ```
- **看板搭建** [scripts/superset_hutt_shop_dashboard.py](scripts/superset_hutt_shop_dashboard.py)
  (幂等,存在则更新):注册 `mart.v_hutt_shop_orders` 数据集 → 建 8 张图
  (KPI 大数字×3:净收入/订单数/客单价;周净收入&订单折线;各产品净收入&销量柱状;
  支付方式饼图;地区净收入表;折扣码表现表)→ 组装看板 `Hutt Online Dashboard`
  (slug=`hutt-online-shop`)。
  ```bash
  docker run --rm --network channelhub_channelhub --env-file .env \
    -v "$PWD/scripts/superset_hutt_shop_dashboard.py:/dash.py:ro" \
    prefecthq/prefect:3-latest python /dash.py
  # 完成后访问 https://<服务器IP>/superset/dashboard/hutt-online-shop/
  ```
- 数据出现多行项订单后(Shopify 导出的订单级金额只落首行),需把视图拆成
  订单层/行项层两个 —— 见 009 迁移头部注释,当前按 YAGNI 未预建。

### 看板迭代工作流(本地改 → push → 自动上线)

看板呈现是**代码定义、幂等重放**的:你只改本地的 `scripts/superset_*_dashboard.py`(图、
布局、标题、指标…),push 后由 **deploy 自动重建服务器看板**,不用上服务器跑任何东西。

```
本地改 superset_expert_dashboard.py  →  git push main
        │
        ▼  GitHub Action(.github/workflows/deploy.yml)
   git pull + docker compose up --build
        │
        ▼  bash scripts/superset_provision.sh   ← deploy 自动调用
   等 Superset 就绪 → 应用 BI 口径视图 → 确保 admin/数据源
                    → 重跑所有 superset_*_dashboard.py(create-or-update)
        │
        ▼  服务器看板即时更新 ✅
```

- **唯一供给脚本** [scripts/superset_provision.sh](scripts/superset_provision.sh):幂等,被
  deploy 和 [scripts/initialize.sh](scripts/initialize.sh) 共用(单一事实源)。
- **加新看板**:在 `scripts/` 下按 `superset_<名字>_dashboard.py` 命名,通配自动纳入,push 即上线。
- **加新 BI 口径视图**:写成 `CREATE OR REPLACE VIEW` 的迁移,加进 `superset_provision.sh`
  顶部的 `IDEMPOTENT_MIGRATIONS` 数组即可(别放会 DROP 重塑 core 视图链的 002/003/005)。
- **加新 BI 参照 seed**(如 PLZ→州):写个幂等 loader,加进 `BI_SEED_LOADERS` 数组,deploy 自动重放。
- **改动是持久的**:看板存 Superset 元数据库、视图存 channelhub 库,都在持久卷里,容器重建不丢。
- **失败即判部署失败**(`script_stop:true`)——刻意给即时反馈;想"看板脚本出错不挡部署",
  把 deploy.yml 里那行改成 `bash scripts/superset_provision.sh || true`。
- ⚠️ 服务器 `.env` **别设** `SUPERSET_BIND=0.0.0.0`(留默认 `127.0.0.1`,走 Caddy HTTPS)。

> 手动单跑(本地或服务器,效果等同 deploy 那步):`bash scripts/superset_provision.sh`

### Metabase(备用 BI,SSH 隧道)

- Metabase 仍在跑,端口 `127.0.0.1:3000`,**不**对外。需要时本机开 SSH 隧道:
  ```bash
  ssh -N -L 3000:localhost:3000 deploy@<服务器IP>
  # 浏览器开 http://localhost:3000
  ```
- 自动化脚本 [scripts/metabase_setup.py](scripts/metabase_setup.py) 仍由 initialize.sh 调用,
  保留管理员/数据源/Overview 仪表盘已建好的状态。两套 BI 共享同一 Postgres + 同一 `bi_readonly`。

## 路线图

1. ~~RAW 层 `raw.sell_through_expert`~~ ✅；后续接入 MSD/Telekom（加 raw 表 + 表头签名）
2. ~~邮件解析 ETL：.eml → raw.*（带血缘）~~ ✅ 已完成（见「解析 ETL flow」）
3. ~~Prefect flow：邮件源文件备份到 MinIO~~ ✅ 已完成（见「邮箱备份 flow」）
4. ~~CORE 规范化 + 去重层（dim/fact 视图）~~ ✅ 已完成（见「数据分层」去重语义）
5. ~~Metabase 报表与仪表盘 + BI 只读角色~~ ✅ 已完成（见「可视化」）
6. ~~GTIN 白名单：只放行自家产品进 core~~ ✅ 已完成（见「GTIN 白名单」）
7. ~~mart 物化为真实表（GTIN 粒度 + 运营公司维）+ 解析 flow 末尾链式刷新~~ ✅
   已完成（见「MART 物化层」）
8. ~~Superset PSI 看板（P 由库存恒等式从相邻期推出）~~ ✅ 已完成（见「PSI 看板」）
9. ~~邮件服务：收信 → 处理 → 回信（规则注册表 + 不回自己的信）~~ ✅ 已完成
   （见「邮件服务 flow」）
10. 后续：接入更多供应商（加 raw 表 + 表头签名，自动并入 core/mart）；LLM 控图层

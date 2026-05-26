# ChannelHub — 进销存数据平台（本地开发）

本地开发阶段可运行的进销存数据平台基础设施。当前为**基础设施轮次**：只搭好
数据库、对象存储与配套工具的编排，**暂不含业务表 DDL**。后续轮次再做表结构、
Python 邮件 ETL、Prefect flow 与 Metabase 报表。

## 服务一览

| 服务 | 镜像 | 访问地址 | 用途 |
|---|---|---|---|
| PostgreSQL | `postgres:16` | `localhost:5432` | 主业务库 `channelhub` + `metabase` + `prefect` 支撑库（同一实例） |
| pgAdmin | `dpage/pgadmin4:latest` | http://localhost:5050 | 数据库可视化管理（已预注册服务器 ChannelHub-PG） |
| MinIO | `minio/minio:latest` | API http://localhost:9000 / 控制台 http://localhost:9001 | 邮件源文件备份对象存储，桶 `email-archive` |
| Metabase | `metabase/metabase:latest` | http://localhost:3000 | BI 报表 |
| Prefect 3 OSS | `prefecthq/prefect:3-latest` | 本机 http://localhost:4200 ；**远程经 Caddy 用 https** | 任务编排 server |
| Caddy | `caddy:2` | https://<PREFECT_PROXY_HOST>（默认 https://192.168.178.73） | 给 Prefect UI 套自签 TLS，修复远程白屏（见「故障排查」） |

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
```

首启 PostgreSQL 时会自动执行 `db/init/00_create_support_databases.sql`，
创建 `metabase`、`prefect` 两个**空支撑库**（不含业务表）。

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
   其余 `EMAIL_*` 已给好默认（`imap.ionos.com:993` SSL、仅 INBOX、每小时整点）。
   SMTP 也已写入 `.env` 留作以后发信，本 flow 不用。
2. 起服务（首次会 build worker 镜像）：
   ```bash
   docker compose up -d --build
   ```
3. 在 Prefect UI（http://localhost:4200）→ Deployments 可见 **email-backup**，
   已带每小时 cron；点 **Run** 可立即手动触发一次，运行历史在 UI 可查。

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
# 006 在 core 之上新建 mart 物化层，不取代视图链）
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
所有 `.eml` → 取 `.xlsx` 附件（排除签名内嵌图）→ `openpyxl` 读表头与
`SUPPLIER_REGISTRY` 表头签名比对：

- 命中 **Expert** → 逐行带血缘 `INSERT ... ON CONFLICT DO NOTHING` 进
  `raw.sell_through_expert`（幂等，取值原样 TEXT，不规范化）
- 任何签名都不命中 → 经 SMTP（`smtp.ionos.de:465` SSL）给 `ALERT_EMAIL_TO`
  （收件人地址配置在 `.env`，不入库）发告警邮件；`raw.ingest_alert` 去重，
  同一未识别文件只告警一次
- 新增供应商 = `SUPPLIER_REGISTRY` 加一条表头签名 + 一个 `raw.sell_through_<x>` 表

编排：Prefect deployment **parse-sell-through**，**无独立 cron** —— 由
**email-backup flow 成功后链式触发**（`run_deployment`，timeout=0 非阻塞），
保证「备份完立刻解析、解析完立刻刷新 mart」一条链。改 `.env` 的备份 cron 后重新
注册同邮箱备份方式（`docker compose up -d --build --force-recreate prefect-deploy prefect-worker`）。

## 可视化（Metabase + 只读角色）

- **只读角色** `bi_readonly`（[db/migrations/004_bi_readonly_role.sql](db/migrations/004_bi_readonly_role.sql)）：
  仅 `SELECT` raw+core（006 起含 mart），无写权限。Metabase（及日后 LLM）用它连库，权限隔离。
  密码不入迁移文件，应用后单独设：
  ```bash
  docker compose exec -T postgres psql -U channelhub -d channelhub \
    -v ON_ERROR_STOP=1 -f - < db/migrations/004_bi_readonly_role.sql
  docker compose exec -T postgres psql -U channelhub -d channelhub \
    -c "ALTER ROLE bi_readonly PASSWORD '$BI_READONLY_PASSWORD'"   # 值取自 .env
  ```
- **Metabase 自动化** [scripts/metabase_setup.py](scripts/metabase_setup.py)（仅标准库，幂等）：
  建管理员（凭据见 `.env` 的 `MB_ADMIN_EMAIL/PASSWORD`）→ 用 `bi_readonly` 接
  `channelhub` 库 → 建多张问题 → 组 **ChannelHub Overview** 仪表盘
  （读 `mart.*`，库存为主：总件数 / 产品数(GTIN) / Top15 门店 / 按运营公司 /
  按 ISO 周 / Top15 产品 / 当前快照明细）。
  卡片/仪表盘标题与图表列名均为**英文**；脚本按英文名查重，开头会先按已知旧
  中文名幂等清掉历史中文卡片/仪表盘（DELETE 不支持则归档），重跑即自动迁移。
  ```bash
  # 用 docker --env-file 读 .env(字面解析,不要 source：.env 里 CRON 的
  # "0 * * * *" 未加引号,被 shell 当 glob 会误跑命令;source 也不 export)
  docker run --rm --network channelhub_channelhub \
    --env-file .env \
    -v "$PWD/scripts/metabase_setup.py:/mb.py:ro" \
    prefecthq/prefect:3-latest python /mb.py
  ```
- 访问：http://localhost:3000 （管理员见 `.env`）→ 仪表盘 **ChannelHub Overview**。
  数据走 `bi_readonly` → `mart` 真实表（按 GTIN 去重+规范化的物化口径，
  由 parse flow 末尾刷新；core 视图仍可作实时核对）。

## 路线图

1. ~~RAW 层 `raw.sell_through_expert`~~ ✅；后续接入 MSD/Telekom（加 raw 表 + 表头签名）
2. ~~邮件解析 ETL：.eml → raw.*（带血缘）~~ ✅ 已完成（见「解析 ETL flow」）
3. ~~Prefect flow：邮件源文件备份到 MinIO~~ ✅ 已完成（见「邮箱备份 flow」）
4. ~~CORE 规范化 + 去重层（dim/fact 视图）~~ ✅ 已完成（见「数据分层」去重语义）
5. ~~Metabase 报表与仪表盘 + BI 只读角色~~ ✅ 已完成（见「可视化」）
6. ~~GTIN 白名单：只放行自家产品进 core~~ ✅ 已完成（见「GTIN 白名单」）
7. ~~mart 物化为真实表（GTIN 粒度 + 运营公司维）+ 解析 flow 末尾链式刷新~~ ✅
   已完成（见「MART 物化层」）
8. 后续：接入更多供应商（加 raw 表 + 表头签名，自动并入 core/mart）；LLM 控图层

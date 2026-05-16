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
| Prefect 3 OSS | `prefecthq/prefect:3-latest` | http://localhost:4200 | 任务编排 server |

> 镜像统一用滚动 tag 以保证可拉取；如需可复现环境，请在 `docker-compose.yml`
> 中固定为具体版本 tag。

## 前置条件

- 已安装 **Docker Engine + Docker Compose 插件**（`docker compose version` 可用）。
  > 本机当前未安装 Docker，需先安装：参见
  > https://docs.docker.com/engine/install/ ，并将当前用户加入 `docker` 组。
- 端口 `5432 / 5050 / 9000 / 9001 / 3000 / 4200` 未被占用（如冲突见下文「故障排查」）。

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
- **Prefect 页面报 `Oops. Something went wrong.`**：UI 是 SPA，需用浏览器可达的
  API 地址。compose 已设 `PREFECT_UI_API_URL=/api`（相对路径），无论经 localhost
  还是局域网 IP 访问都能连通。仍报错时**强制刷新浏览器清缓存**（Ctrl+Shift+R），
  旧版前端会缓存错误的 API 地址。

## 路线图（后续轮次，本轮不做）

1. 业务表结构 DDL（产品 / 颜色 / SKU / 渠道 / 门店 / 库存余额 / 进销存流水 /
   邮件导入记录 / 去重记录）
2. Python 邮件抓取 / 解析 / ETL
3. Prefect flow（含将邮件源文件备份写入 MinIO `email-archive` 桶）
4. Metabase 报表与仪表盘

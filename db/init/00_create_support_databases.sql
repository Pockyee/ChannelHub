-- 基础设施初始化（非业务表）。
-- 仅在 postgres 数据卷为空的首次启动时由官方镜像入口自动执行一次。
-- 用途：为 Metabase 与 Prefect 各创建一个独立的空应用库。
-- 业务九类对象（产品/颜色/SKU/渠道/门店/库存余额/进销存流水/邮件导入/去重）
-- 不在此处创建，留待后续单独一轮 DDL 设计。

SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

SELECT 'CREATE DATABASE prefect'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prefect')\gexec

# ChannelHub 部署 & CI/CD

单 VM + docker compose 的部署方案。CI 在 GitHub Actions 跑代码校验;CD 推送
到 `main` 后,GitHub Actions 通过 SSH 登录服务器执行 `git pull` +
`docker compose up -d --build`。

## 总览

```
开发者 push                                      服务器(单 VM)
    │                                              ┌────────────────────┐
    ▼                                              │ docker compose 栈   │
GitHub  ─── PR ──► CI(语法 + compose 校验)        │  postgres / minio   │
        │                                          │  prefect / superset │
        └── main push ──► CD ── SSH ──► git pull ─▶│  caddy / worker     │
                                  + compose up -d  └────────────────────┘
```

凭据策略:
- `.env`(IMAP/SMTP/数据库密码等)**只存在于服务器**,既不进 git 也不进 CI。
- GitHub Secrets **只**存 SSH 私钥和服务器连接信息。
- 每次部署 = 服务器上 `git pull` + `docker compose up -d --build`,镜像在
  服务器本地构建,不走外部 registry。

## 一、首次部署(全新服务器)

### 1. 在服务器跑 bootstrap 脚本

用 root 或带 sudo 的用户 SSH 进去,执行:

```bash
# 上传脚本后(或直接 curl):
sudo bash scripts/bootstrap_server.sh
```

脚本会:
- 装 Docker Engine + Compose 插件
- 创建 `deploy` 用户并加入 `docker` 组
- `git clone` 仓库到 `/opt/channelhub`
- 生成 `.env`,基础设施密码用 `openssl rand -hex 24` 随机填,业务凭据
  (EMAIL_USER/PASSWORD、SMTP_*、PREFECT_PROXY_HOST、ALERT_EMAIL_TO)
  打了 `CHANGE_ME` 占位,需要手动补。

补完 `.env` 后第一次手动起来:

```bash
sudo -u deploy bash -lc "cd /opt/channelhub && docker compose up -d"
sudo -u deploy bash -lc "cd /opt/channelhub && docker compose ps"
```

### 2. 生成 SSH 部署密钥(本机执行,不在服务器)

```bash
ssh-keygen -t ed25519 -f ~/.ssh/channelhub_deploy -C channelhub-deploy -N ''
```

把公钥追加到服务器 `deploy` 用户的 `authorized_keys`:

```bash
ssh-copy-id -i ~/.ssh/channelhub_deploy.pub deploy@<SERVER_IP>
```

(首次会要 `deploy` 用户密码——bootstrap 脚本没设密码,可先在服务器
`sudo passwd deploy` 设一个临时的,或者直接 `cat ~/.ssh/channelhub_deploy.pub`
复制贴到服务器的 `/home/deploy/.ssh/authorized_keys`。)

验证免密能通:

```bash
ssh -i ~/.ssh/channelhub_deploy deploy@<SERVER_IP> "docker compose -f /opt/channelhub/docker-compose.yml ps"
```

### 3. 在 GitHub 仓库配置 Secrets

打开 `Settings → Secrets and variables → Actions → New repository secret`,
加这几条:

| Secret 名 | 内容 |
|---|---|
| `DEPLOY_HOST` | 服务器 IP 或域名 |
| `DEPLOY_USER` | `deploy` |
| `DEPLOY_PATH` | `/opt/channelhub` |
| `DEPLOY_SSH_KEY` | `cat ~/.ssh/channelhub_deploy` 的**私钥**全文(含 `-----BEGIN...` 行) |
| `DEPLOY_PORT` | 仅当 SSH 不是 22 才填 |

可选: 在 `Settings → Environments` 新建一个名为 `production` 的 environment,
开启 "Required reviewers" 让部署需要人工点确认,工作流已经配了
`environment: production`。

### 4. 测试自动部署

往 `main` 推一个无害改动(比如改 README 一行字),或在 Actions 页面手动跑
`Deploy` 工作流。看绿勾即成功。

## 二、日常开发流程

1. 在分支上改代码 → 开 PR → CI(语法 + compose 校验)跑过
2. Code review → merge 到 `main`
3. GitHub Actions 自动 SSH 上服务器拉新代码、rebuild、健康检查
4. 失败会在 Actions 日志里打 Prefect 容器最近 80 行日志,方便排查

## 三、常见问题

**Q: SSH 失败,提示 Permission denied (publickey).**
- 检查 `DEPLOY_SSH_KEY` 是否粘了**私钥**(不是公钥),且含开头结尾的
  `-----BEGIN/END OPENSSH PRIVATE KEY-----` 行。
- 在服务器 `cat /home/deploy/.ssh/authorized_keys` 确认公钥在里面。
- 服务器 `/etc/ssh/sshd_config` 是否禁了 `PubkeyAuthentication`。

**Q: git pull 报本地有未提交改动.**
- 服务器上不应该有手改的文件。如果是 `.env` 之外的文件被改了,先在服务器
  `git status` 看清楚再决定 `git stash` 还是丢弃。CD 用的是 `--ff-only`,
  有冲突会主动失败,不会盲目覆盖你的现场。

**Q: 镜像越攒越多撑爆磁盘.**
- workflow 末尾有 `docker image prune -f`,但只清悬空层。如需更狠的清理
  可定期手动 `docker system prune -af --volumes`(注意 `--volumes` 会删
  没在用的数据卷,慎用)。

**Q: 想回滚.**
- 服务器上手动 `cd /opt/channelhub && git checkout <旧 commit>` 然后
  `docker compose up -d --build`。或者在 GitHub 上 revert 那个 PR,
  让 CD 自动把代码退回到上一版本。

## 四、CI/CD 文件位置

- [.github/workflows/ci.yml](../.github/workflows/ci.yml) — PR/非 main push 触发,跑语法 + compose 校验
- [.github/workflows/deploy.yml](../.github/workflows/deploy.yml) — main push / 手动触发,SSH 部署
- [scripts/bootstrap_server.sh](../scripts/bootstrap_server.sh) — 全新服务器一次性初始化

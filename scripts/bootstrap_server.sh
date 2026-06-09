#!/usr/bin/env bash
# 全新 Ubuntu/Debian 服务器一次性初始化脚本
# 在目标服务器 root(或 sudo)身份下执行:
#   curl -fsSL https://raw.githubusercontent.com/Pockyee/ChannelHub/main/scripts/bootstrap_server.sh | sudo bash
# 或先 git clone 再 sudo bash scripts/bootstrap_server.sh
#
# 完成后:
#   - 安装 Docker Engine + Compose 插件
#   - 创建 deploy 用户(用于 GitHub Actions SSH 部署)
#   - 把代码 clone 到 /opt/channelhub
#   - 生成 .env 模板(随机密码已填,业务凭据需手动补)
# 注意: 这是一次性脚本; CI/CD 不会调用它。

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/Pockyee/ChannelHub.git}"
DEPLOY_USER="${DEPLOY_USER:-deploy}"
DEPLOY_PATH="${DEPLOY_PATH:-/opt/channelhub}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "请用 root 或 sudo 执行: sudo bash $0" >&2
  exit 1
fi

echo "==> 1/6 apt update"
apt-get update -y
apt-get install -y ca-certificates curl gnupg git ufw openssl

echo "==> 2/6 安装 Docker Engine + Compose 插件(官方源)"
if ! command -v docker >/dev/null 2>&1; then
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  chmod a+r /etc/apt/keyrings/docker.gpg

  . /etc/os-release
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
    > /etc/apt/sources.list.d/docker.list

  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin
  systemctl enable --now docker
else
  echo "  docker 已存在,跳过"
fi

echo "==> 3/6 创建 ${DEPLOY_USER} 用户并加入 docker 组"
if ! id "${DEPLOY_USER}" >/dev/null 2>&1; then
  useradd -m -s /bin/bash "${DEPLOY_USER}"
fi
usermod -aG docker "${DEPLOY_USER}"
install -d -m 0700 -o "${DEPLOY_USER}" -g "${DEPLOY_USER}" \
  "/home/${DEPLOY_USER}/.ssh"
touch "/home/${DEPLOY_USER}/.ssh/authorized_keys"
chown "${DEPLOY_USER}:${DEPLOY_USER}" "/home/${DEPLOY_USER}/.ssh/authorized_keys"
chmod 0600 "/home/${DEPLOY_USER}/.ssh/authorized_keys"

echo "==> 4/6 clone 仓库到 ${DEPLOY_PATH}"
if [[ ! -d "${DEPLOY_PATH}/.git" ]]; then
  git clone "${REPO_URL}" "${DEPLOY_PATH}"
fi
chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "${DEPLOY_PATH}"

echo "==> 5/6 生成 .env(若不存在)"
ENV_FILE="${DEPLOY_PATH}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  PG_PWD=$(openssl rand -hex 24)
  PGADMIN_PWD=$(openssl rand -hex 24)
  MINIO_PWD=$(openssl rand -hex 24)
  BI_PWD=$(openssl rand -hex 24)
  MB_PWD=$(openssl rand -hex 16)Aa1!

  install -m 0600 -o "${DEPLOY_USER}" -g "${DEPLOY_USER}" /dev/null "${ENV_FILE}"
  cat > "${ENV_FILE}" <<EOF
# 由 bootstrap_server.sh 自动生成; 业务凭据(IMAP/SMTP)需手动补
POSTGRES_USER=channelhub
POSTGRES_PASSWORD=${PG_PWD}
POSTGRES_DB=channelhub
POSTGRES_PORT=5432

PGADMIN_DEFAULT_EMAIL=admin@channelhub.com
PGADMIN_DEFAULT_PASSWORD=${PGADMIN_PWD}
PGADMIN_PORT=5050

MINIO_ROOT_USER=channelhub
MINIO_ROOT_PASSWORD=${MINIO_PWD}
MINIO_BUCKET=email-archive
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001

METABASE_PORT=3000

PREFECT_PORT=4200
PREFECT_PROXY_HOST=CHANGE_ME_PUBLIC_IP_OR_DOMAIN
PREFECT_HTTPS_PORT=443

EMAIL_HOST=imap.ionos.de
EMAIL_PORT=993
EMAIL_USER=CHANGE_ME
EMAIL_PASSWORD=CHANGE_ME
EMAIL_FOLDERS=INBOX
EMAIL_BACKUP_PREFIX=email
EMAIL_BACKUP_CRON=0 * * * *
EMAIL_PARSE_CRON=30 * * * *
EMAIL_BACKUP_MINIO_ENDPOINT=minio:9000

SMTP_HOST=smtp.ionos.de
SMTP_PORT=465
SMTP_USER=CHANGE_ME
SMTP_PASSWORD=CHANGE_ME
ALERT_EMAIL_TO=CHANGE_ME

BI_READONLY_USER=bi_readonly
BI_READONLY_PASSWORD=${BI_PWD}

MB_ADMIN_EMAIL=admin@channelhub.com
MB_ADMIN_PASSWORD=${MB_PWD}
EOF
  chown "${DEPLOY_USER}:${DEPLOY_USER}" "${ENV_FILE}"
  chmod 0600 "${ENV_FILE}"
  echo "  .env 已生成,务必手动填入 EMAIL_* / SMTP_* / PREFECT_PROXY_HOST"
else
  echo "  ${ENV_FILE} 已存在,保留不动"
fi

echo "==> 6/6 完成"
cat <<HINT

下一步(在本机执行,不在服务器):
  1) 生成 GitHub Actions 用的 SSH keypair:
       ssh-keygen -t ed25519 -f ~/.ssh/channelhub_deploy -C channelhub-deploy -N ''
  2) 把公钥追加到服务器的 ${DEPLOY_USER} 用户:
       ssh-copy-id -i ~/.ssh/channelhub_deploy.pub ${DEPLOY_USER}@<SERVER_IP>
     (首次用密码,后续就用 key)
  3) 在 GitHub 仓库 Settings -> Secrets and variables -> Actions 添加:
       DEPLOY_HOST    = <SERVER_IP>
       DEPLOY_USER    = ${DEPLOY_USER}
       DEPLOY_PATH    = ${DEPLOY_PATH}
       DEPLOY_SSH_KEY = (cat ~/.ssh/channelhub_deploy 的私钥全文)
       DEPLOY_PORT    = 22  (非默认才填)
  4) 回到服务器,先手动跑一次起来:
       sudo -u ${DEPLOY_USER} bash -lc "cd ${DEPLOY_PATH} && docker compose up -d"
  5) 然后 push 到 main 就会自动部署。

HINT

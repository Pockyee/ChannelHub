# WireGuard VPN（wg-easy）

让笔记本、手机等设备用**官方 WireGuard 客户端**连上 ChannelHub 服务器，
所有上网流量从服务器出口出去 —— 对外呈现的就是这台 VM 的**固定公网 IP**
`82.165.180.145`。

## 为什么装在这台机器

| | 家里那台（`psi`） | **IONOS VM（生产）** |
|---|---|---|
| 公网 IP | `93.233.39.158`，**动态**，随时会变 | `82.165.180.145`，**固定** |
| 网络位置 | Fritz!Box 后面，需端口转发 + DDNS | 公网 IP 直接绑在 `ens6`（`/32`），无 NAT |
| 适合做 VPN 出口 | ❌ IP 会变，做不到"固定 IP" | ✅ |

所以 wg-easy 加在 `docker-compose.yml` 里，跟着现有的 GitHub Actions
部署流程一起上生产，无需单独运维。

## 架构

```
你的电脑                       IONOS VM (82.165.180.145)
┌──────────────┐              ┌──────────────────────────────────┐
│ WireGuard    │  UDP 51820   │ wg-easy 容器                      │
│ 官方客户端    │─────────────▶│  wg0 (10.8.0.1)                  │
│ 10.8.0.2     │              │   │ MASQUERADE                    │
└──────────────┘              │   ▼                               │
                              │  eth0 → docker bridge → ens6 ─────┼──▶ 互联网
   全部流量                    │                                   │    出口 IP =
   AllowedIPs=0.0.0.0/0       │  管理 UI :51821 (仅 127.0.0.1)    │    82.165.180.145
                              └──────────────────────────────────┘
```

## 一、上线步骤

### 1. 在服务器补 `.env`

`.env` 只存在于服务器，不进 git。SSH 进去追加 `WG_*` 段：

```bash
ssh -i ~/.ssh/channelhub_deploy deploy@82.165.180.145
cd /opt/channelhub

# 生成管理 UI 密码并记下来（这是你唯一一次能明文看到它）
WG_PW=$(openssl rand -hex 24); echo "wg-easy 管理密码: $WG_PW"

cat >> .env <<EOF

# ---- WireGuard VPN（wg-easy）----
WG_HOST=82.165.180.145
WG_ADMIN_USERNAME=admin
WG_ADMIN_PASSWORD=$WG_PW
WG_PORT=51820
WG_DNS=1.1.1.1
WG_IPV4_CIDR=10.8.0.0/24
WG_ALLOWED_IPS=0.0.0.0/0
WG_UI_BIND=127.0.0.1
WG_UI_PORT=51821
WG_INSECURE=true
EOF
```

> ⚠️ `WG_ADMIN_PASSWORD` **只在首次启动时生效**。`wg_easy_data` 卷一旦建好，
> 改 `.env` 不再有任何影响 —— 之后改密码要进管理 UI 改。

各变量含义见 `.env.example` 里的注释。

### 2. 在 IONOS Cloud Panel 放行 UDP 51820

**这一步必须手动做，容器和脚本都碰不到它。** IONOS 的防火墙策略在虚拟机
外面，配置在 Cloud Panel：

> Cloud Panel → Network → Firewall Policies → 选中这台 VM 的策略 →
> 加一条入站规则：**协议 UDP，端口 51820，允许来源 任意**

没做这步的症状：客户端一直卡在握手，`wg show` 里 `latest handshake` 永远空白。

关于服务器上的 `ufw`（当前是 active）：**不需要为 51820 加规则**。Docker 发布
端口走的是 `nat/PREROUTING` + `DOCKER` 链，位置在 ufw 的过滤链之前，所以
Docker 发布的端口天然绕过 ufw。（这是 Docker + ufw 的已知行为，不是这里的
特例——也正因如此，别指望 ufw 能拦住任何 `docker compose` 发布的端口。）

### 3. 部署

推到 `main` 即可，现有的 Deploy workflow 会自动 `git pull` + `docker compose up -d --build`：

```bash
git push origin main
```

或者在服务器上手动起：

```bash
cd /opt/channelhub && docker compose up -d wg-easy
docker compose logs -f wg-easy      # 看到 "Wireguard Interface wg0 started successfully"
docker compose ps wg-easy           # 等到 healthy
```

## 二、添加一台客户端设备

管理 UI 只绑 `127.0.0.1`（明文 HTTP，不能公网暴露），所以从你本机开 SSH 隧道访问 ——
跟 Prefect 是同一套路：

```bash
ssh -N -L 51821:localhost:51821 -i ~/.ssh/channelhub_deploy deploy@82.165.180.145
```

保持这个终端开着，浏览器打开 <http://localhost:51821>，用 `.env` 里的
`WG_ADMIN_USERNAME` / `WG_ADMIN_PASSWORD` 登录。

然后：

1. **New Client** → 起个名（如 `q-work`、`macbook`）→ Create
2. 手机：直接用 WireGuard app **扫二维码**
3. 电脑：点下载拿到 `.conf` 文件 → WireGuard 客户端 *Import tunnel from file*

每台设备建**各自独立**的 client，别几台共用一份配置 —— WireGuard 一个 peer
同时只能有一个活跃 endpoint，共用会导致互相踢线。

## 三、验证出口 IP

客户端连上后，在**客户端**上执行：

```bash
curl -4 https://ifconfig.co
# 期望输出：82.165.180.145
```

若返回的还是你本地 ISP 的 IP，说明 full tunnel 没生效 —— 检查客户端配置里的
`AllowedIPs` 是不是 `0.0.0.0/0`。

## 四、切换 split tunnel（只走内网，不改出口）

如果某台设备只想用 VPN 访问服务器内部服务、平时上网仍走本地：改客户端
`.conf` 里的：

```ini
AllowedIPs = 172.18.0.0/16, 10.8.0.0/24
```

服务端默认值由 `.env` 的 `WG_ALLOWED_IPS` 控制（只影响**之后新建**的 client）。

## 五、排错

| 症状 | 原因 / 处理 |
|---|---|
| 握手不成功（`latest handshake` 空白） | 十有八九是 IONOS Cloud Panel 没放行 UDP 51820，见步骤 2 |
| 连上了但不能上网 | 进容器看 NAT：`docker compose exec wg-easy iptables -t nat -S POSTROUTING`，应有 `-s 10.8.0.0/24 -o eth0 -j MASQUERADE` |
| 容器起不来，日志报 IPv6 | `channelhub` 这张 bridge 没开 IPv6，compose 里已设 `DISABLE_IPV6=true`，别改回去 |
| 忘了管理 UI 密码 | 改 `.env` 没用。要么进 UI 改，要么删卷重来：`docker compose down wg-easy && docker volume rm channelhub_wg_easy_data && docker compose up -d wg-easy`（⚠️ 所有已配好的客户端会全部作废，需重新添加） |
| 想换隧道端口 | 改 `.env` 的 `WG_PORT`，同步改 IONOS 防火墙规则，并**重新下发**客户端配置（Endpoint 端口变了） |

查看当前连了哪些设备：

```bash
docker compose exec wg-easy wg show
```

## 六、几点提醒

- **全部流量走这台 VM**：带宽、流量配额都算在这台 IONOS 机器上，看视频、下大
  文件都会跑满它的上行。介意的话用 split tunnel。
- 出口是**数据中心 IP**，不是住宅 IP。少数网站（银行、流媒体）会对数据中心
  IP 区别对待。
- 这是你自己的服务器，目的是**固定 IP**，不是匿名 —— 流量对服务器本身是可见的。
- 管理 UI 是明文 HTTP，`WG_UI_BIND` 请保持 `127.0.0.1`，永远走 SSH 隧道访问。

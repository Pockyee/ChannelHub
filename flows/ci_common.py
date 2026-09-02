"""ChannelHub — 竞品情报(CI)采集公共层：抓取 / 归档 / 型号消歧 / 告警。

本文件是 ci_* 系列 flow 的**共用底座**，只被 flows/ci_*.py 导入。
(仓库惯例「小工具在各 flow 文件内各备一份、不跨 flow import」针对的是跨业务线；
 同一业务线内共享底座是必要的 —— 抓取合规与型号消歧只能有一份实现。)

四块内容：
  · fetch()          四级升级抓取(api/http/impersonate/browser) + robots + 限速 + 路径白名单
  · snapshot()       正文原样存 MinIO(桶 ci-archive) + raw.ci_snapshot 去重登记
  · match_product()  型号消歧 —— 全项目唯一「错了不报错、只给错结论」的地方
  · alert / unmatched  复用 raw.ingest_alert 的去重告警惯例

所有外部依赖(psycopg / minio / curl_cffi / httpx)一律**函数内延迟导入**：
本模块只用标准库即可 import，好让 scripts/check_ci_matching.py 在任何环境
(含未装依赖的开发机)都能验证消歧逻辑 —— 那是最该随手能跑的一个测试。
"""

from __future__ import annotations

import hashlib
import os
import re
import smtplib
import threading
import time
import urllib.robotparser
from dataclasses import dataclass
from datetime import date, datetime, timezone
from email.message import EmailMessage
from urllib.parse import urlparse


# ===========================================================================
# 一、基础设施(沿用仓库惯例：_env / _pg / _minio 同名同形)
# ===========================================================================
def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _flag(name: str, default: str = "false") -> bool:
    """compose 在 .env 缺键时注入**空字符串**，os.environ.get 的 default 不生效。

    故必须 (_env(x) or default)，见 flows/mail_service.py 的同类处理。
    """
    return (_env(name) or default).lower() in ("true", "t", "1", "yes")


def is_dry_run() -> bool:
    """CI_DRY_RUN=true(默认)：照常抓取解析，但不写库、不存 MinIO。"""
    return (_env("CI_DRY_RUN") or "true").lower() != "false"


def _pg():
    import psycopg  # 延迟导入：消歧逻辑不该依赖 DB 驱动
    return psycopg.connect(
        host=_env("POSTGRES_HOST", "postgres"),
        port=int(_env("POSTGRES_PORT", "5432") or "5432"),
        dbname=_env("POSTGRES_DB", "channelhub"),
        user=_env("POSTGRES_USER"),
        password=_env("POSTGRES_PASSWORD"),
    )


def _minio() -> "Minio":
    from minio import Minio  # 延迟导入(同上)
    return Minio(
        _env("MINIO_ENDPOINT", "minio:9000"),
        access_key=_env("MINIO_ACCESS_KEY"),
        secret_key=_env("MINIO_SECRET_KEY"),
        secure=_env("MINIO_SECURE", "false").lower() == "true",
    )


def ci_bucket() -> str:
    return _env("CI_MINIO_BUCKET", "ci-archive")


def ensure_bucket(client: "Minio", bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


# ===========================================================================
# 二、抓取：robots + 限速 + 路径白名单 + 四级升级
# ===========================================================================
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 (+ChannelHub competitive-intel; {contact})"
)

# 每域最小请求间隔(秒)。总量本就很小(日 500–2000)，慢一点换稳定得多。
DOMAIN_MIN_INTERVAL = float(_env("CI_MIN_INTERVAL_SEC") or "2.5")

# Amazon 路径白名单：只允许商品页与评论页 —— 二者均不在 amazon.de robots.txt 的
# User-agent:* Disallow 列表内。搜索页 /s? 反爬强度高一个量级且我们已知 ASIN，
# 明确禁用。其余 /gp/ 等一律不碰。
AMAZON_ALLOWED = (re.compile(r"^/dp/[A-Z0-9]{10}/?$"), re.compile(r"^/product-reviews/[A-Z0-9]{10}/?$"))

_last_hit: dict[str, float] = {}
_hit_lock = threading.Lock()
_robots_cache: dict[str, urllib.robotparser.RobotFileParser | None] = {}


class FetchBlocked(RuntimeError):
    """被 robots / 路径白名单 / 反爬拦下 —— 与网络错误区分开，便于分别告警。"""


def _throttle(domain: str) -> None:
    with _hit_lock:
        prev = _last_hit.get(domain, 0.0)
        wait = DOMAIN_MIN_INTERVAL - (time.monotonic() - prev)
        if wait > 0:
            time.sleep(wait)
        _last_hit[domain] = time.monotonic()


def _fetch_robots_text(origin: str) -> str | None:
    """用**真实 UA / TLS 指纹**取 robots.txt。

    绝不能用 RobotFileParser.read()：它以 Python 默认 UA 直接 urlopen，反爬站点
    (idealo=Akamai)会回 403，而 RobotFileParser 按 RFC 把 401/403 解释成
    **禁止一切** —— 于是一个 robots 实际允许的源会被永久静默关掉，日志还写着
    「robots.txt 禁止」，完全误导。取 robots 这一步本身也必须能过反爬。
    """
    contact = _env("CI_CONTACT_EMAIL") or _env("ALERT_EMAIL_TO") or "unknown"
    hdrs = {"User-Agent": USER_AGENT.format(contact=contact),
            "Accept-Language": "de-DE,de;q=0.9"}
    for getter in ("impersonate", "http"):
        try:
            if getter == "impersonate":
                from curl_cffi import requests as creq
                r = creq.get(f"{origin}/robots.txt", headers=hdrs,
                             timeout=20, impersonate="chrome")
            else:
                import httpx
                r = httpx.get(f"{origin}/robots.txt", headers=hdrs,
                              timeout=20, follow_redirects=True)
            if r.status_code == 200:
                return r.text
        except Exception:
            continue
    return None


def _robots_allows(url: str) -> bool:
    """robots.txt 的机器可读 opt-out —— 欧盟 TDM 例外依赖它，故是硬性检查。

    取不到 robots.txt 时**放行**并记为未知：取不到 ≠ 禁止。真正的禁止只能来自
    一份成功读到、且明确 Disallow 的 robots.txt。
    """
    parsed = urlparse(url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin not in _robots_cache:
        text = _fetch_robots_text(origin)
        if text is None:
            _robots_cache[origin] = None
        else:
            rp = urllib.robotparser.RobotFileParser()
            rp.parse(text.splitlines())
            _robots_cache[origin] = rp
    rp = _robots_cache[origin]
    if rp is None:
        return True
    return rp.can_fetch("*", url)


def check_path_allowed(url: str) -> None:
    """站点特有的路径收敛(目前只有 Amazon)。不合规直接抛 FetchBlocked。"""
    parsed = urlparse(url)
    if parsed.netloc.endswith("amazon.de"):
        if not any(p.match(parsed.path) for p in AMAZON_ALLOWED):
            raise FetchBlocked(f"Amazon 路径不在白名单: {parsed.path}")


BOT_WALL_MARKERS = (
    "captcha", "robot check", "are you a human", "geben sie die zeichen ein",
    "enable javascript", "access denied", "unusual traffic",
    # JS 挑战页会以 **HTTP 200** 返回一个几 KB 的空壳，既没有验证码字样也没有错误码。
    # 不认出来就会被当成「今天没数据」，在价格曲线上留下假的断点。用容器 id 这类
    # 高特异性标记，不要用 "akamai"/"cloudflare" 这种词 —— 正常页面的 CDN 资源
    # URL 里就有它们，会误伤。
    "sec-if-cpt-container", "sec-bc-tile-container",      # Akamai Bot Manager
    "cf-browser-verification", "cf_chl_", "challenge-platform",  # Cloudflare
    "_incapsula_resource",                                 # Imperva
)


def looks_like_bot_wall(body: str, status: int) -> bool:
    """反爬墙识别 —— 必须显式失败告警，不能静默存一条空记录。"""
    if status in (403, 429, 503):
        return True
    head = body[:4000].lower()
    return any(m in head for m in BOT_WALL_MARKERS)


def fetch(url: str, *, mode: str = "http", headers: dict | None = None,
          timeout: float = 30.0, params: dict | None = None) -> tuple[int, bytes, str]:
    """统一抓取入口。返回 (status, body_bytes, content_type)。

    mode 四级升级：
      api        —— 官方 API/JSON，跳过 robots(接口非爬取)，仍限速
      http       —— 普通 httpx GET
      impersonate—— curl_cffi TLS 指纹伪装，对付 DataDome/Cloudflare
      browser    —— 预留：Playwright。v1 不实现，会显式抛错而不是悄悄降级。
    """
    parsed = urlparse(url)
    domain = parsed.netloc
    contact = _env("CI_CONTACT_EMAIL") or _env("ALERT_EMAIL_TO") or "unknown"
    hdrs = {
        "User-Agent": USER_AGENT.format(contact=contact),
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.6",
    }
    hdrs.update(headers or {})

    if mode != "api":
        check_path_allowed(url)
        if not _robots_allows(url):
            raise FetchBlocked(f"robots.txt 禁止: {url}")

    _throttle(domain)

    if mode == "browser":
        raise FetchBlocked(
            "browser 模式(Playwright)在 v1 未实现 —— 见计划:引入它需同时拆出独立 "
            "work pool 与镜像，不要在此处悄悄降级到 impersonate"
        )

    if mode == "impersonate":
        from curl_cffi import requests as creq  # 延迟导入
        r = creq.get(url, headers=hdrs, params=params, timeout=timeout,
                     impersonate="chrome", allow_redirects=True)
        return r.status_code, r.content, r.headers.get("content-type", "")

    import httpx  # 延迟导入
    r = httpx.get(url, headers=hdrs, params=params, timeout=timeout,
                  follow_redirects=True)
    return r.status_code, r.content, r.headers.get("content-type", "")


# ===========================================================================
# 三、归档：原样存 MinIO + raw.ci_snapshot 去重登记
# ===========================================================================
def content_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _ext_for(content_type: str) -> str:
    ct = (content_type or "").lower()
    if "json" in ct:
        return "json"
    if "xml" in ct:
        return "xml"
    if "html" in ct:
        return "html"
    return "bin"


def snapshot(source_code: str, url: str, status: int, body: bytes,
             content_type: str, *, product_id: str | None = None,
             run_id: str = "") -> int | None:
    """存档并登记。内容未变(content_hash 命中)则不重复存，返回已有 snapshot_id。

    dry-run 下只算哈希不落任何东西，返回 None。
    """
    h = content_hash(body)
    if is_dry_run():
        return None

    key = f"ci/{source_code}/{date.today().isoformat()}/{h[:16]}.{_ext_for(content_type)}"
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT snapshot_id FROM raw.ci_snapshot "
                "WHERE source_code=%s AND url=%s AND content_hash=%s",
                (source_code, url, h),
            )
            row = cur.fetchone()
            if row:
                return row[0]               # 内容没变 —— 不重复存、不重复送 LLM

    client = _minio()
    bucket = ci_bucket()
    ensure_bucket(client, bucket)
    import io
    client.put_object(bucket, key, io.BytesIO(body), length=len(body),
                      content_type=content_type or "application/octet-stream")

    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.ci_snapshot "
                "(source_code, product_id, url, http_status, content_hash, object_key, "
                " content_type, byte_size, ingestion_run_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (source_code, url, content_hash) DO NOTHING "
                "RETURNING snapshot_id",
                (source_code, product_id, url, status, h, key,
                 content_type, len(body), run_id),
            )
            row = cur.fetchone()
            if row is None:                 # 并发下被别人抢先插入
                cur.execute(
                    "SELECT snapshot_id FROM raw.ci_snapshot "
                    "WHERE source_code=%s AND url=%s AND content_hash=%s",
                    (source_code, url, h),
                )
                row = cur.fetchone()
        conn.commit()
    return row[0] if row else None


# ===========================================================================
# 四、型号消歧 —— 全项目唯一「错了不报错、只给错结论」的地方
# ===========================================================================
# 配件/耗材词：命中即判定不是整机，避免 "HUTT 10 Ersatztücher 10 Stück" 被算成一台机器。
ACCESSORY_TOKENS = (
    "ersatz", "tuch", "tücher", "tuecher", "pad", "wischtuch", "zubehör", "zubehoer",
    "reinigungsmittel", "reiniger", "halterung", "netzteil", "kabel", "fernbedienung",
    "sicherheitsseil", "filter", "bürste", "buerste", "set aus", "nachfüll", "nachfuell",
    "spare", "replacement", "accessor",
)


@dataclass(frozen=True)
class Product:
    product_id: str
    brand: str
    display_name: str
    is_own: bool
    ean: str | None
    brand_re: re.Pattern | None
    model_re: re.Pattern | None


def load_products(active_only: bool = True) -> list[Product]:
    sql = ("SELECT product_id, brand, display_name, is_own, ean, brand_regex, match_regex "
           "FROM core.ci_product")
    if active_only:
        sql += " WHERE active"
    sql += " ORDER BY product_id"
    out: list[Product] = []
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for pid, brand, disp, own, ean, brx, mrx in cur.fetchall():
                out.append(Product(
                    pid, brand, disp, own, ean,
                    re.compile(brx, re.IGNORECASE) if brx else None,
                    re.compile(mrx, re.IGNORECASE) if mrx else None,
                ))
    return out


def is_accessory(text: str) -> bool:
    t = (text or "").lower()
    return any(tok in t for tok in ACCESSORY_TOKENS)


def match_product(text: str, products: list[Product]) -> tuple[str | None, str]:
    """标题/正文 → product_id。返回 (product_id|None, reason)。

    规则(顺序即优先级)：
      1. EAN 命中 —— 最可靠，直接返回
      2. 配件词命中 —— 判定为耗材，不是整机
      3. 品牌正则 AND 型号正则 同时命中
      4. 命中两款以上 → **判为歧义并返回 None**，绝不猜。
         W2 / W2S / W2 PRO 互为前缀，猜错不会报错、只会悄悄给出错误结论。
    """
    if not text:
        return None, "empty"
    raw = text
    low = text.lower()

    for p in products:                       # 1) EAN 最优先
        if p.ean and p.ean in raw:
            return p.product_id, "ean"

    if is_accessory(low):                    # 2) 配件/耗材
        return None, "accessory"

    hits = [p.product_id for p in products
            if p.brand_re and p.model_re
            and p.brand_re.search(raw) and p.model_re.search(raw)]

    if not hits:
        return None, "no_match"
    if len(set(hits)) > 1:                   # 4) 歧义不猜
        return None, "ambiguous:" + ",".join(sorted(set(hits)))
    return hits[0], "regex"


def match_products_all(text: str, products: list[Product]) -> list[str]:
    """文档语境(文章/讨论帖/视频)：返回**所有**命中的 product_id。

    与 match_product() 的分工是本模块最要紧的区分：
      · 商品页/报价 是单品语境 —— 歧义必须判失败，猜错会污染价格与销量序列
      · 文章/讨论 是文档语境 —— 一篇对比评测本来就该同时挂到多款上，
        raw.ci_mention 的唯一键含 product_id 正是为此
    """
    if not text:
        return []
    if is_accessory(text.lower()):
        return []
    hits = []
    for p in products:
        if p.ean and p.ean in text:
            hits.append(p.product_id)
            continue
        if p.brand_re and p.model_re and p.brand_re.search(text) and p.model_re.search(text):
            hits.append(p.product_id)
    return sorted(set(hits))


def record_unmatched(source_code: str, external_id: str, title: str, url: str) -> None:
    """匹配失败进待审队列(见过多次只累加计数，不刷屏)。"""
    if is_dry_run():
        return
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.ci_unmatched (source_code, external_id, raw_title, url) "
                "VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (source_code, external_id) DO UPDATE SET "
                "  seen_count = raw.ci_unmatched.seen_count + 1, last_seen_at = now()",
                (source_code, external_id, (title or "")[:1000], url),
            )
        conn.commit()


# ===========================================================================
# 五、GDPR：作者身份只留加盐哈希
# ===========================================================================
def author_hash(author: str | None) -> str | None:
    """评论作者名是个人数据 —— 只存 salt+名 的哈希，绝不落库显示名。"""
    if not author:
        return None
    salt = _env("CI_AUTHOR_SALT")
    if not salt:
        raise RuntimeError("CI_AUTHOR_SALT 未配置：作者身份不可明文入库")
    return hashlib.sha256(f"{salt}|{author.strip().lower()}".encode()).hexdigest()


# ===========================================================================
# 六、告警(复用 raw.ingest_alert 的去重惯例与 [ChannelHub] 主题前缀)
# ===========================================================================
def _send_alert(subject: str, body: str) -> None:
    host = _env("SMTP_HOST", "smtp.ionos.de")
    port = int(_env("SMTP_PORT", "465") or "465")
    user = _env("SMTP_USER")
    pw = _env("SMTP_PASSWORD")
    to = _env("ALERT_EMAIL_TO")
    if not (user and pw and to):
        raise RuntimeError("SMTP_USER/SMTP_PASSWORD/ALERT_EMAIL_TO 未配置，无法发告警")
    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = to
    msg["Subject"] = subject
    msg["Date"] = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
    msg.set_content(body)
    with smtplib.SMTP_SSL(host, port, timeout=30) as s:
        s.login(user, pw)
        s.send_message(msg)


def maybe_alert(source_code: str, reason: str, detail: str, logger) -> None:
    """同一(源, 原因)当天只告警一次 —— 复用 raw.ingest_alert 去重表。

    source_object_key 借用为 'ci/<source>/<日期>'，source_file_name 借用为 reason，
    这样天然复用既有的 uq_ingest_alert 唯一约束，不必另建一张告警表。
    """
    key = f"ci/{source_code}/{date.today().isoformat()}"
    if is_dry_run():
        logger.warning("[dry-run] 本应告警: %s / %s — %s", source_code, reason, detail)
        return
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM raw.ingest_alert WHERE source_object_key=%s AND source_file_name=%s",
                (key, reason),
            )
            if cur.fetchone():
                logger.info("告警已发过，跳过: %s / %s", source_code, reason)
                return
    _send_alert(
        f"[ChannelHub] 竞品情报采集异常: {source_code} / {reason}",
        f"源: {source_code}\n原因: {reason}\n\n{detail}\n",
    )
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.ingest_alert "
                "(source_object_key, source_file_name, reason, header_seen) "
                "VALUES (%s,%s,%s,%s) ON CONFLICT (source_object_key, source_file_name) DO NOTHING",
                (key, reason, reason, detail[:4000]),
            )
        conn.commit()
    logger.warning("已发送采集告警: %s / %s", source_code, reason)

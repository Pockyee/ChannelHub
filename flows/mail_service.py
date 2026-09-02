"""ChannelHub — 邮件服务 flow：收信 → 处理 → 回信（MinIO 已备份 .eml 为输入）。

与 parse_sell_through 的分工：那条是「把附件吞进 raw.* 的单向 ETL」，这条是
「读懂来信、生成文件、回信给发件人」的请求-应答服务。两者都读同一批 .eml，
但失败语义完全不同（对外发信必须 at-most-once），所以分成两个 flow。

扩展方式：往 RULES 加一条规则 + 一个 handler 即可，flow 骨架不用动。

两道护栏（**必须先于规则匹配执行**）：
  · 不回自己的信 —— From == EMAIL_USER/SMTP_USER 直接跳过。
    这条不是可选项：ai-sunrise 规则匹配的是「发件人以 ai-sunrise.de 结尾」，
    而 data@ai-sunrise.de 自己也满足，没这道闸就是自触发死循环。
  · 不回自动信 —— Auto-Submitted / Precedence: bulk / List-Id / 我们自己发信
    时打的 X-ChannelHub-Rule 头，命中任一即跳过（防和对方的自动回复对打）。

幂等：先在 raw.mail_request 占坑(INSERT ON CONFLICT DO NOTHING)再发信。
占坑失败=已处理过，直接跳过。发信中途失败的行留在 'processing' 且**不自动
重试**（重试可能变成重复发信）—— 宁可漏发也不重复轰炸，同时发告警。
"""

from __future__ import annotations

import csv
import email as email_lib
import io
import os
import re
import smtplib
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import formatdate, make_msgid, parseaddr

import psycopg
from minio import Minio
from prefect import flow, get_run_logger, task


# ===========================================================================
# 一、共用基础设施：env / MinIO / Postgres / 发信
#     （沿用仓库现有惯例：小工具在各 flow 文件内各备一份，不跨 flow import）
# ===========================================================================
def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _norm(h) -> str:
    """表头归一：去掉非字母数字、转小写 —— 'Shipping Zip' → 'shippingzip'。"""
    return re.sub(r"[^0-9a-z]", "", str(h).strip().lower()) if h is not None else ""


def _decode(v) -> str:
    try:
        return str(make_header(decode_header(v))) if v else ""
    except Exception:
        return str(v)


def _addr(v) -> str:
    """From/To 头 → 纯小写邮件地址（丢掉显示名）。"""
    return parseaddr(_decode(v))[1].strip().lower()


def _minio() -> Minio:
    return Minio(
        _env("MINIO_ENDPOINT", "minio:9000"),
        access_key=_env("MINIO_ACCESS_KEY"),
        secret_key=_env("MINIO_SECRET_KEY"),
        secure=_env("MINIO_SECURE", "false").lower() == "true",
    )


def _pg():
    return psycopg.connect(
        host=_env("POSTGRES_HOST", "postgres"),
        port=int(_env("POSTGRES_PORT", "5432") or "5432"),
        dbname=_env("POSTGRES_DB", "channelhub"),
        user=_env("POSTGRES_USER"),
        password=_env("POSTGRES_PASSWORD"),
    )


def _is_production() -> bool:
    """CHANNELHUB_ENV 未设置/空串一律按 **production** —— 生产的 .env 不动也不受影响。"""
    return (_env("CHANNELHUB_ENV") or "production").lower() == "production"


def _dry_run() -> bool:
    """只有显式写 false 才真发信；未设置/空串/乱填一律当 dry run。

    安全侧必须是「不发」：compose 里 ${MAIL_SERVICE_DRY_RUN} 在 .env 缺这一项时
    会注入**空串**（键存在但为空），os.environ.get 的默认值这时不生效 —— 用
    `== "true"` 判断的话，服务器上漏配一行就会静悄悄开始真发信。

    ⚠️ **非生产环境无条件 dry run，MAIL_SERVICE_DRY_RUN=false 也翻不开。**
    测试机跑的是同一套真实邮箱凭据（EMAIL_USER/SMTP_USER 指向真实业务邮箱），
    「测试环境给真实客户发了信」必须在结构上不可能发生，而不是靠记得改一个开关
    ——那个开关恰恰最容易在从生产拷 .env 时被一起拷过来。
    """
    if not _is_production():
        return True
    return (_env("MAIL_SERVICE_DRY_RUN") or "true").lower() != "false"


def _smtp_conf() -> tuple[str, int, str, str]:
    host = _env("SMTP_HOST", "smtp.ionos.de")
    port = int(_env("SMTP_PORT", "465") or "465")
    user = _env("SMTP_USER")
    pw = _env("SMTP_PASSWORD")
    if not (user and pw):
        raise RuntimeError("SMTP_USER / SMTP_PASSWORD 未配置，无法发信")
    return host, port, user, pw


def _send_alert(subject: str, body: str) -> None:
    """内部告警（发给固定的 ALERT_EMAIL_TO），与 parse_sell_through 同款。"""
    host, port, user, pw = _smtp_conf()
    to = _env("ALERT_EMAIL_TO")
    if not to:
        raise RuntimeError("ALERT_EMAIL_TO 未配置，无法发告警")
    msg = EmailMessage()
    msg["From"] = user
    msg["To"] = to
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg.set_content(body)
    with smtplib.SMTP_SSL(host, port, timeout=30) as s:
        s.login(user, pw)
        s.send_message(msg)


def build_reply(orig: email_lib.message.Message, to_addr: str, rule_key: str,
                body: str, attachment: tuple[str, bytes] | None,
                from_addr: str) -> EmailMessage:
    """拼回信（可带一个 CSV 附件）。与发送分开，便于离线校验邮件头。

    带 Auto-Submitted 让对方邮件系统知道这是自动信（别再自动回过来）；
    带 X-ChannelHub-Rule 让**我们自己的护栏**认出这封是自己发的 —— 万一它被
    抄送/转发回 INBOX，下一轮扫描会直接跳过，不会二次触发。
    """
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    subject = _decode(orig.get("Subject")) or "(kein Betreff)"
    msg["Subject"] = subject if subject.lower().startswith("re:") else f"Re: {subject}"
    orig_id = (orig.get("Message-ID") or "").strip()
    if orig_id:
        msg["In-Reply-To"] = orig_id
        msg["References"] = orig_id
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()
    msg["Auto-Submitted"] = "auto-replied"
    msg["X-ChannelHub-Rule"] = rule_key
    msg.set_content(body)
    if attachment:
        fn, data = attachment
        msg.add_attachment(data, maintype="text", subtype="csv", filename=fn)
    return msg


def _send_reply(orig: email_lib.message.Message, to_addr: str, rule_key: str,
                body: str, attachment: tuple[str, bytes] | None, logger) -> None:
    host, port, user, pw = _smtp_conf()
    msg = build_reply(orig, to_addr, rule_key, body, attachment, user)

    if _dry_run():
        logger.warning(
            "[DRY RUN] 不实际发信 —— 收件人=%s 主题=%r 附件=%s(%d 字节)",
            to_addr, msg["Subject"],
            attachment[0] if attachment else "无",
            len(attachment[1]) if attachment else 0,
        )
        return
    with smtplib.SMTP_SSL(host, port, timeout=30) as s:
        s.login(user, pw)
        s.send_message(msg)
    logger.info("已回信 → %s（附件 %s）", to_addr,
                attachment[0] if attachment else "无")


# ===========================================================================
# 二、护栏：任何规则匹配之前先跑这一段
# ===========================================================================
_AUTO_HEADERS = ("X-ChannelHub-Rule", "List-Id", "List-Unsubscribe", "X-Autoreply")


def skip_reason(msg: email_lib.message.Message) -> str:
    """返回跳过原因；空字符串 = 这封信可以进规则匹配。"""
    sender = _addr(msg.get("From"))
    own = {a for a in (_env("EMAIL_USER").lower(), _env("SMTP_USER").lower()) if a}
    if sender and sender in own:
        return f"自发信（From={sender}，不回自己的信）"

    auto = (msg.get("Auto-Submitted") or "").strip().lower()
    if auto and auto != "no":
        return f"自动信（Auto-Submitted: {auto}）"
    prec = (msg.get("Precedence") or "").strip().lower()
    if prec in ("bulk", "junk", "list", "auto_reply"):
        return f"自动信（Precedence: {prec}）"
    for h in _AUTO_HEADERS:
        if msg.get(h):
            return f"自动信（存在 {h} 头）"
    return ""


# ===========================================================================
# 三、规则 ai_sunrise_orders_export
#     Shopify orders_export.csv → 物流用 ai-sunrise-DDMMYYYY.csv（分号分隔）
# ===========================================================================
# 签名只取核心列子集：Shopify 不同版本导出会增减边缘列，核心列在才认定是订单导出
ORDERS_EXPORT_REQUIRED = {
    "name", "email", "financialstatus", "currency", "total", "createdat",
    "lineitemquantity", "lineitemname", "lineitemprice", "lineitemsku",
}

OUT_HEADER = ["Referenznummer", "Bestelldatum", "Stück", "Produktname", "SKU",
              "Name", "Straße", "PLZ", "Ort", "Land", "Zusatz"]

REF_PREFIX = "ais_"

# 行项目级列（每行都有值）
_C_ORDER, _C_QTY, _C_ITEM, _C_SKU = "name", "lineitemquantity", "lineitemname", "lineitemsku"
# 订单级列（Shopify 只在每单**第一行**填，后续行项目行为空 → 必须前向填充）
_C_CREATED = "createdat"
_C_SHIP_NAME = "shippingname"
_C_ADDR1, _C_ADDR2 = "shippingaddress1", "shippingaddress2"
_C_ZIP, _C_CITY, _C_COUNTRY, _C_COMPANY = (
    "shippingzip", "shippingcity", "shippingcountry", "shippingcompany")
_ORDER_LEVEL = (_C_CREATED, _C_SHIP_NAME, _C_ADDR1, _C_ADDR2,
                _C_ZIP, _C_CITY, _C_COUNTRY, _C_COMPANY)


def _unwrap(row: list[str], ncols: int) -> list[str]:
    """整行被多包了一层引号时再解一层。

    实际收到的 Shopify 导出长这样：表头正常 79 列，但**每行数据被整行包进一对
    引号**、内部引号双写（`""`）。csv.reader 会把这样一行读成**一个字段**，
    行项目列全取不到 → 静默产出 0 行空文件。这里检测到"只有 1 个字段但表头有
    多列"就把那个字段的内容再当一行 CSV 解一遍。

    只在解出来确实变多列时才采纳，所以对正常格式的文件是无害的。
    """
    if len(row) == 1 and ncols > 1 and row[0].strip():
        inner = next(csv.reader(io.StringIO(row[0])), None)
        if inner and len(inner) > 1:
            return inner
    return row


def _read_csv(payload: bytes) -> tuple[list[str], list[list[str]]]:
    """→ (表头, 数据行)。两种格式都吃：标准 CSV，和整行多包一层引号的变体。"""
    reader = csv.reader(io.StringIO(payload.decode("utf-8-sig", errors="replace")))
    rows = list(reader)
    if not rows:
        return [], []
    header = rows[0]
    if len(header) == 1:                      # 表头本身也被包了
        header = _unwrap(header, 79)
    ncols = len(header)
    return header, [_unwrap(r, ncols) for r in rows[1:]]


def _header_index(header: list[str]) -> dict[str, int]:
    idx: dict[str, int] = {}
    for i, h in enumerate(header):
        n = _norm(h)
        if n and n not in idx:       # 同名列取第一个
            idx[n] = i
    return idx


def is_orders_export(fn: str, payload: bytes) -> bool:
    """.csv 且表头命中 Shopify 订单导出签名。"""
    if not fn.lower().endswith(".csv"):
        return False
    try:
        header, _ = _read_csv(payload)
    except Exception:
        return False
    if not header:
        return False
    return ORDERS_EXPORT_REQUIRED <= {n for n in _header_index(header)}


def _bestelldatum(created: str) -> str:
    """'2026-08-24 13:18:57 +0200' → '24.08.26'（DD.MM.YY）；解析不了就原样。"""
    s = (created or "").strip()
    if len(s) >= 10:
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%d.%m.%y")
        except ValueError:
            pass
    return s


def _strasse(addr1: str, addr2: str) -> str:
    """Address1 + Address2 合并；只在两边都非空时才加分隔符（不留尾巴）。"""
    return ", ".join(p for p in (addr1.strip(), addr2.strip()) if p)


def _plz(v: str) -> str:
    """去掉 Excel 防丢前导零加的撇号；**不补零**（AT 邮编是 4 位，如 6414）。"""
    return v.strip().lstrip("'").strip()


def _reference(order_name: str) -> str:
    """'#1547' → 'ais_1547'。"""
    return REF_PREFIX + order_name.strip().lstrip("#").strip()


def build_ai_sunrise_rows(payload: bytes) -> list[list[str]]:
    """orders_export.csv → 输出数据行（不含表头）。

    核心难点是**订单级字段的前向填充**：Shopify 导出里一个订单的地址/日期只出现在
    该订单的第一行，后续行项目那些列全是空的；而输出要求每个行项目都带完整地址。
    所以走两遍：先按订单号收集每个订单级字段的第一个非空值，再按**原始行序**输出。

    不做任何业务过滤 —— 取消单、未付款单都照样输出（1:1 转换）。
    """
    header, data_rows = _read_csv(payload)
    if not header:
        raise ValueError("空 CSV：没有表头行")
    idx = _header_index(header)

    def cell(row: list[str], key: str) -> str:
        i = idx.get(key)
        return row[i].strip() if i is not None and i < len(row) else ""

    items: list[tuple[str, str, str, str]] = []
    ctx: dict[str, dict[str, str]] = {}
    for row in data_rows:
        if not any(c.strip() for c in row):
            continue                                   # 尾部空行
        order = cell(row, _C_ORDER)
        qty, item = cell(row, _C_QTY), cell(row, _C_ITEM)
        if not order or (not item and not qty):
            continue                                   # 不是行项目行
        c = ctx.setdefault(order, {})
        for k in _ORDER_LEVEL:
            v = cell(row, k)
            if v and not c.get(k):
                c[k] = v                               # 该单第一个非空值胜出
        items.append((order, qty, item, cell(row, _C_SKU)))

    out = []
    for order, qty, item, sku in items:
        c = ctx.get(order, {})
        out.append([
            _reference(order),
            _bestelldatum(c.get(_C_CREATED, "")),
            qty,
            item,
            sku,
            c.get(_C_SHIP_NAME, ""),
            _strasse(c.get(_C_ADDR1, ""), c.get(_C_ADDR2, "")),
            _plz(c.get(_C_ZIP, "")),
            c.get(_C_CITY, ""),
            c.get(_C_COUNTRY, ""),
            c.get(_C_COMPANY, ""),
        ])
    return out


def render_csv(rows: list[list[str]]) -> bytes:
    """分号分隔 + UTF-8 BOM —— 德语 Excel 双击即可正确打开（BOM 不能省）。"""
    buf = io.StringIO(newline="")
    w = csv.writer(buf, delimiter=";", quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
    w.writerow(OUT_HEADER)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")


def _now_local() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Berlin"))
    except Exception:                    # 容器缺 tzdata → 退回 UTC（日界差 1-2h，可接受）
        return datetime.now(timezone.utc)


def handle_ai_sunrise_orders(fn: str, payload: bytes) -> tuple[str, bytes, str, int]:
    """→ (附件名, 附件字节, 回信正文, 数据行数)。"""
    rows = build_ai_sunrise_rows(payload)
    out_name = f"ai-sunrise-{_now_local().strftime('%d%m%Y')}.csv"
    body = (
        f"Hallo,\n\n"
        f"anbei die aufbereitete Bestellliste ({len(rows)} Positionen) "
        f"aus der Datei {fn}.\n\n"
        f"Diese Nachricht wurde automatisch von ChannelHub erzeugt.\n"
    )
    return out_name, render_csv(rows), body, len(rows)


# ===========================================================================
# 四、规则注册表 —— 将来加邮件功能：加一条规则 + 一个 handler，flow 骨架不动
# ===========================================================================
RULES = [
    {
        "key": "ai_sunrise_orders_export",   # 也是 raw.mail_request 的去重键之一
        "from_domain": "ai-sunrise.de",      # 发件人地址后缀
        "match_attachment": is_orders_export,
        "handler": handle_ai_sunrise_orders,
        "reject_body": (
            "Hallo,\n\nleider konnte der Anhang nicht als Shopify-Bestellexport "
            "(orders_export.csv) erkannt werden. Es wurde keine Datei erzeugt.\n\n"
            "Diese Nachricht wurde automatisch von ChannelHub erzeugt.\n"
        ),
        "empty_body": (
            "Hallo,\n\nder Anhang wurde als Shopify-Bestellexport erkannt, es "
            "konnte daraus aber keine einzige Bestellposition gelesen werden. "
            "Es wurde keine Datei erzeugt.\n\n"
            "Diese Nachricht wurde automatisch von ChannelHub erzeugt.\n"
        ),
    },
]


def match_rule(sender: str):
    for rule in RULES:
        if sender.endswith("@" + rule["from_domain"]):
            return rule
    return None


# ===========================================================================
# 五、记账：raw.mail_request（先占坑再发信 → at-most-once）
# ===========================================================================
def handled_object_keys() -> set[str]:
    """已处理过的 .eml 对象键 —— 扫描时命中即跳过，连 .eml 都不下载。"""
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT source_object_key FROM raw.mail_request")
            return {r[0] for r in cur.fetchall()}


def claim(rule_key: str, object_key: str, msg_id: str, sender: str, subject: str) -> bool:
    """占坑成功返回 True；False = 已被处理过（或正在处理），不要再发信。"""
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.mail_request "
                "(rule_key, source_object_key, email_message_id, email_from, "
                " email_subject, status) VALUES (%s,%s,%s,%s,%s,'processing') "
                "ON CONFLICT (rule_key, source_object_key) DO NOTHING",
                (rule_key, object_key, msg_id, sender, subject),
            )
            claimed = cur.rowcount == 1
        conn.commit()
    return claimed


def finish(rule_key: str, object_key: str, status: str, detail: str | None = None,
           reply_file_name: str | None = None, rows_out: int | None = None) -> None:
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE raw.mail_request SET status=%s, detail=%s, "
                "reply_file_name=%s, rows_out=%s, replied_at=now() "
                "WHERE rule_key=%s AND source_object_key=%s",
                (status, detail, reply_file_name, rows_out, rule_key, object_key),
            )
        conn.commit()


# ===========================================================================
# 六、编排
# ===========================================================================
@task(retries=2, retry_delay_seconds=20)
def list_eml_keys() -> list[str]:
    c = _minio()
    bucket = _env("MINIO_BUCKET", "email-archive")
    return [
        o.object_name
        for o in c.list_objects(bucket, prefix="email/", recursive=True)
        if o.object_name.endswith(".eml")
    ]


@task(retries=0)          # 刻意不重试：对外发信 at-most-once，重试可能重复发
def handle_eml(object_key: str) -> dict:
    logger = get_run_logger()
    bucket = _env("MINIO_BUCKET", "email-archive")
    stats = {"matched": 0, "replied": 0, "unrecognized": 0, "skipped": 0, "failed": 0}

    raw = _minio().get_object(bucket, object_key).read()
    msg = email_lib.message_from_bytes(raw)

    reason = skip_reason(msg)
    if reason:
        stats["skipped"] += 1
        logger.info("跳过 %s：%s", object_key, reason)
        return stats

    sender = _addr(msg.get("From"))
    rule = match_rule(sender)
    if not rule:
        stats["skipped"] += 1
        return stats

    subject = _decode(msg.get("Subject"))
    msg_id = (msg.get("Message-ID") or "").strip()

    # 找第一个命中签名的 .csv 附件
    hit = None
    saw_csv = False
    for part in msg.walk():
        fn = _decode(part.get_filename() or "")
        if not fn.lower().endswith(".csv"):
            continue
        payload = part.get_payload(decode=True) or b""
        if not payload:
            continue
        saw_csv = True
        if rule["match_attachment"](fn, payload):
            hit = (fn, payload)
            break

    if not saw_csv:
        # @ai-sunrise.de 同事日常抄送 data@ 的普通邮件 —— 不记账、不回信
        stats["skipped"] += 1
        return stats

    stats["matched"] += 1
    if not claim(rule["key"], object_key, msg_id, sender, subject):
        stats["skipped"] += 1
        logger.info("已处理过，跳过（不重复发信）：%s", object_key)
        return stats

    try:
        if hit is None:
            _send_reply(msg, sender, rule["key"], rule["reject_body"], None, logger)
            finish(rule["key"], object_key, "unrecognized", "csv 表头未命中订单导出签名")
            stats["unrecognized"] += 1
            logger.warning("附件未识别，已回信说明：%s", object_key)
            return stats

        fn, payload = hit
        out_name, out_bytes, body, n_rows = rule["handler"](fn, payload)
        if n_rows == 0:
            # 表头认出来了却一行都没读到 —— 多半是没见过的格式变体。
            # 绝不能把只有表头的空文件当成正常结果发出去（收件人会照着空文件发货）。
            _send_reply(msg, sender, rule["key"], rule["empty_body"], None, logger)
            finish(rule["key"], object_key, "empty", f"来源附件 {fn}，解析出 0 行")
            stats["unrecognized"] += 1
            logger.error("%s 解析出 0 行，已回信说明且**不发空文件**：%s", fn, object_key)
            _send_alert(
                f"[ChannelHub] 邮件服务解析出 0 行: {fn}",
                f"对象: {object_key}\n发件人: {sender}\n附件: {fn}\n\n"
                f"表头命中订单导出签名，但一行数据都没读出来 —— 多半是没见过的\n"
                f"格式变体。已回信告知发件人，未发送空文件。请检查该附件格式。",
            )
            return stats
        _send_reply(msg, sender, rule["key"], body, (out_name, out_bytes), logger)
        # dry run 也照常占坑记账（这正是首次上线"消化历史积压"的手段：跑一遍
        # dry run，历史邮件就都记上账了，之后切成真发信不会突然给几个月前的
        # 旧邮件回信）。但状态如实记 dry_run，别谎称 replied。
        finish(rule["key"], object_key, "dry_run" if _dry_run() else "replied",
               f"来源附件 {fn}", out_name, n_rows)
        stats["replied"] += 1
        logger.info("%s → 回信 %s（%d 行）给 %s", fn, out_name, n_rows, sender)
    except Exception as exc:
        # 行留在 'processing'，不自动重试（重试可能变成重复发信）。
        # 要重发：手工 DELETE 掉那行再跑本 flow。
        stats["failed"] += 1
        logger.error("处理 %s 失败：%s", object_key, exc)
        try:
            _send_alert(
                f"[ChannelHub] 邮件服务处理失败: {rule['key']}",
                f"对象: {object_key}\n发件人: {sender}\n主题: {subject}\n"
                f"错误: {exc}\n\n"
                f"该行留在 raw.mail_request 的 processing 状态且不会自动重试。\n"
                f"确认无误后 DELETE 掉该行再重跑 mail-service 即可重发。",
            )
        except Exception as alert_exc:
            logger.error("告警也发不出去：%s", alert_exc)
        raise
    return stats


@flow(name="mail-service")
def mail_service() -> dict:
    logger = get_run_logger()
    if not _is_production():
        logger.warning("CHANNELHUB_ENV=%s（非 production）—— **强制** dry run，"
                       "本环境永不对外发信；MAIL_SERVICE_DRY_RUN 在此无效",
                       _env("CHANNELHUB_ENV") or "(空)")
    elif _dry_run():
        logger.warning("MAIL_SERVICE_DRY_RUN=true —— 照常生成与记账，但不实际发信")

    keys = list_eml_keys()
    done = handled_object_keys()
    todo = [k for k in keys if k not in done]
    logger.info("归档 .eml %d 封，已处理 %d 封，本轮待看 %d 封",
                len(keys), len(keys) - len(todo), len(todo))

    total = {"matched": 0, "replied": 0, "unrecognized": 0, "skipped": 0, "failed": 0}
    for k in todo:
        try:
            r = handle_eml(k)
            for kk in total:
                total[kk] += r[kk]
        except Exception as e:
            total["failed"] += 1
            logger.warning("处理 %s 失败：%s", k, e)

    logger.info("邮件服务完成：%s", total)
    if total["failed"]:
        raise RuntimeError(f"{total['failed']} 封处理失败，详见日志：{total}")
    return total


if __name__ == "__main__":
    mail_service()

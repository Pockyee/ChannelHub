"""ChannelHub — 报表解析 ETL：MinIO 已备份 .eml → xlsx → 表头识别 → raw.sell_through_<供应商>。

流程：
  扫 MinIO email-archive 所有 .eml（此邮箱只收外来 report）
  → 取每个 .xlsx 附件（排除签名内嵌图）
  → openpyxl 读表头，与“供应商表头签名注册表”比对
     · 命中 Expert        → 逐行带血缘 INSERT ON CONFLICT 进 raw.sell_through_expert（幂等）
     · 任何签名都不命中    → 经 SMTP 给 ALERT_EMAIL_TO 发告警（同一文件只告警一次）
所有取值原样 TEXT 落 raw，不做规范化（解析归一在 core 层）。
"""

from __future__ import annotations

import io
import os
import re
import smtplib
import email as email_lib
from datetime import date, datetime
from email.header import decode_header, make_header
from email.message import EmailMessage

import psycopg
from minio import Minio
from openpyxl import load_workbook
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run


# ---------------------------------------------------------------------------
# 供应商表头签名注册表：normalize(表头) 集合 → (目标表, 列映射)
# 新增供应商：在此加一条签名 + 对应 raw 表即可
# ---------------------------------------------------------------------------
def _norm(h) -> str:
    return re.sub(r"[^0-9a-z]", "", str(h).strip().lower()) if h is not None else ""


EXPERT_COLUMNS = [
    ("periodflag", "period_flag"),
    ("transactiondate", "transaction_date"),
    ("providername", "provider_name"),
    ("company", "company"),
    ("storeid", "store_id"),
    ("store", "store_name"),
    ("street", "street"),
    ("postalcode", "postal_code"),
    ("city", "city"),
    ("customerskucode", "customer_sku_code"),
    ("customerskuname", "customer_sku_name"),
    ("gtinbarcode", "gtin_barcode"),
    ("supplierskucode", "supplier_sku_code"),
    ("soldqtyoutlets", "sold_qty_outlets"),
    ("stockonhandqtyoutlets", "stock_on_hand_qty_outlets"),
]

SUPPLIER_REGISTRY = {
    "expert": {
        "table": "raw.sell_through_expert",
        "columns": EXPERT_COLUMNS,
        "required": {n for n, _ in EXPERT_COLUMNS},
    },
    # 未来：'msd': {...}, 'telekom': {...}
}


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _decode(v) -> str:
    try:
        return str(make_header(decode_header(v))) if v else ""
    except Exception:
        return str(v)


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


def _cell_to_text(v):
    """Excel 单元格 → 忠实字符串；空 → None。不做业务转换。"""
    if v is None:
        return None
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, datetime):
        return v.strftime("%d.%m.%Y")          # 保持文件可见的德式日期形态
    if isinstance(v, date):
        return v.strftime("%d.%m.%Y")
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)  # 不要科学计数/.0
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    return s or None


def _identify(header_norms: list[str]):
    """返回 (supplier_key, spec) 或 (None, None)。"""
    present = {h for h in header_norms if h}
    for key, spec in SUPPLIER_REGISTRY.items():
        if spec["required"] <= present:
            return key, spec
    return None, None


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
    msg.set_content(body)
    with smtplib.SMTP_SSL(host, port, timeout=30) as s:
        s.login(user, pw)
        s.send_message(msg)


@task(retries=2, retry_delay_seconds=20)
def list_eml_keys() -> list[str]:
    c = _minio()
    bucket = _env("MINIO_BUCKET", "email-archive")
    return [
        o.object_name
        for o in c.list_objects(bucket, prefix="email/", recursive=True)
        if o.object_name.endswith(".eml")
    ]


@task(retries=1, retry_delay_seconds=20)
def process_eml(object_key: str) -> dict:
    logger = get_run_logger()
    bucket = _env("MINIO_BUCKET", "email-archive")
    run_id = str(getattr(flow_run, "id", "") or "")
    stats = {"xlsx": 0, "inserted": 0, "skipped": 0, "alerted": 0, "alert_suppressed": 0}

    raw = _minio().get_object(bucket, object_key).read()
    msg = email_lib.message_from_bytes(raw)
    msg_id = (msg.get("Message-ID") or "").strip()
    subject = _decode(msg.get("Subject"))
    sender = _decode(msg.get("From"))

    for part in msg.walk():
        fn = part.get_filename()
        if not fn or not _decode(fn).lower().endswith(".xlsx"):
            continue  # 跳过签名内嵌图(image001/002)及非 xlsx
        fn = _decode(fn)
        if part.get_content_disposition() == "inline" and not fn.lower().endswith(".xlsx"):
            continue
        payload = part.get_payload(decode=True) or b""
        if not payload:
            continue
        stats["xlsx"] += 1

        try:
            wb = load_workbook(io.BytesIO(payload), read_only=True, data_only=True)
        except Exception as e:
            logger.warning("无法打开 xlsx %s (%s): %s", fn, object_key, e)
            _maybe_alert(object_key, fn, "xlsx_open_error", str(e), subject, sender, stats, logger)
            continue

        matched_any = False
        header_seen_repr = ""
        for ws in wb.worksheets:
            header_row_idx, header_norms, header_raw = _find_header(ws)
            if header_row_idx is None:
                continue
            supplier, spec = _identify(header_norms)
            if not supplier:
                header_seen_repr = " | ".join(str(h) for h in header_raw if h is not None)
                continue
            matched_any = True
            ins, skp = _load_sheet(
                ws, header_row_idx, header_norms, spec, object_key, fn,
                msg_id, run_id, logger,
            )
            stats["inserted"] += ins
            stats["skipped"] += skp
            logger.info(
                "解析 %s / sheet=%s 供应商=%s → 入库 %d, 跳过(已存在) %d",
                fn, ws.title, supplier, ins, skp,
            )
        wb.close()

        if not matched_any:
            _maybe_alert(
                object_key, fn, "unrecognized_header", header_seen_repr,
                subject, sender, stats, logger,
            )

    return stats


def _find_header(ws):
    """在前 8 行内找表头行，返回 (1基行号, normalized列表, 原始列表) 或 (None,None,None)。"""
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=8, values_only=True), start=1):
        norms = [_norm(c) for c in row]
        if sum(1 for n in norms if n) >= 5:  # 至少 5 个非空表头才算候选
            return i, norms, list(row)
    return None, None, None


def _load_sheet(ws, header_row_idx, header_norms, spec, object_key, fn, msg_id, run_id, logger):
    col_idx = {}
    for norm, dbcol in spec["columns"]:
        if norm in header_norms:
            col_idx[dbcol] = header_norms.index(norm)
    dbcols = list(col_idx.keys())

    insert_sql = (
        f"INSERT INTO {spec['table']} "
        f"({', '.join(dbcols)}, source_email_message_id, source_object_key, "
        f"source_file_name, source_sheet, source_row_number, ingestion_run_id) "
        f"VALUES ({', '.join(['%s'] * len(dbcols))}, %s, %s, %s, %s, %s, %s) "
        f"ON CONFLICT (source_object_key, source_sheet, source_row_number) DO NOTHING"
    )

    count_sql = (
        f"SELECT count(*) FROM {spec['table']} "
        f"WHERE source_object_key=%s AND source_file_name=%s AND source_sheet=%s"
    )
    cnt_args = (object_key, fn, ws.title)

    attempted = 0
    batch = []
    rownum = header_row_idx
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, cnt_args)
            before = cur.fetchone()[0]
            for row in ws.iter_rows(min_row=header_row_idx + 1, values_only=True):
                rownum += 1
                vals = [_cell_to_text(row[col_idx[c]]) if col_idx[c] < len(row) else None
                        for c in dbcols]
                if all(v is None for v in vals):
                    continue  # 整行空，跳过尾部空行
                attempted += 1
                batch.append(tuple(vals) + (msg_id, object_key, fn, ws.title, rownum, run_id))
                if len(batch) >= 500:
                    cur.executemany(insert_sql, batch)
                    batch.clear()
            if batch:
                cur.executemany(insert_sql, batch)
            conn.commit()
            cur.execute(count_sql, cnt_args)
            after = cur.fetchone()[0]
    inserted = after - before
    skipped = attempted - inserted          # 已存在被 ON CONFLICT 跳过的（幂等可见）
    return inserted, skipped


def _maybe_alert(object_key, fn, reason, header_seen, subject, sender, stats, logger):
    """未识别附件：未告警过才发邮件并记录（去重）。"""
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM raw.ingest_alert WHERE source_object_key=%s AND source_file_name=%s",
                (object_key, fn),
            )
            if cur.fetchone():
                stats["alert_suppressed"] += 1
                logger.info("未识别附件 %s 已告警过，跳过", fn)
                return
    body = (
        f"未能识别报表附件，未入库。\n\n"
        f"附件名: {fn}\n原因: {reason}\n"
        f"邮件主题: {subject}\n发件人: {sender}\n"
        f"MinIO 对象: {object_key}\n\n"
        f"读到的表头:\n{header_seen}\n\n"
        f"请确认该文件格式，或在 SUPPLIER_REGISTRY 增加对应表头签名。"
    )
    _send_alert(f"[ChannelHub] 未识别的报表附件: {fn}", body)
    with _pg() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.ingest_alert "
                "(source_object_key, source_file_name, reason, header_seen, email_subject, email_from) "
                "VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (source_object_key, source_file_name) DO NOTHING",
                (object_key, fn, reason, header_seen, subject, sender),
            )
        conn.commit()
    stats["alerted"] += 1
    logger.warning("已发送未识别附件告警: %s", fn)


@flow(name="parse-sell-through")
def parse_sell_through() -> dict:
    logger = get_run_logger()
    keys = list_eml_keys()
    logger.info("待扫描 .eml: %d", len(keys))

    total = {"xlsx": 0, "inserted": 0, "skipped": 0, "alerted": 0, "alert_suppressed": 0}
    failed = 0
    for k in keys:
        try:
            r = process_eml(k)
            for kk in total:
                total[kk] += r[kk]
        except Exception as e:
            failed += 1
            logger.warning("处理 %s 失败: %s", k, e)

    logger.info("全部完成: %s, failed=%d", total, failed)
    if failed:
        raise RuntimeError(f"{failed} 个 .eml 处理失败，详见日志；其余已入库: {total}")
    return total


if __name__ == "__main__":
    parse_sell_through()

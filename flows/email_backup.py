"""ChannelHub — 邮箱源文件备份 flow（IONOS IMAP → MinIO）。

只读连接、用 BODY.PEEK 抓取，绝不修改/删除/标记已读原邮件。
按 (folder, UIDVALIDITY, UID) 生成确定性对象键，已存在即跳过 —— 幂等、可断点续跑。
所有配置走环境变量（worker 容器从 .env 注入），密钥不进 Prefect 服务端。
"""

from __future__ import annotations

import email
import hashlib
import imaplib
import io
import json
import os
import re
from datetime import datetime, timezone
from email.header import decode_header, make_header

from minio import Minio
from minio.error import S3Error
from prefect import flow, get_run_logger, task


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _decode(value) -> str:
    if value is None:
        return ""
    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def _safe(part: str) -> str:
    """把文件夹名等清洗成对象键安全片段。"""
    return re.sub(r"[^A-Za-z0-9._@-]+", "_", part).strip("_") or "x"


def _minio_client() -> Minio:
    endpoint = _env("MINIO_ENDPOINT", "minio:9000")
    secure = _env("MINIO_SECURE", "false").lower() == "true"
    return Minio(
        endpoint,
        access_key=_env("MINIO_ACCESS_KEY"),
        secret_key=_env("MINIO_SECRET_KEY"),
        secure=secure,
    )


@task(retries=2, retry_delay_seconds=15)
def ensure_bucket(bucket: str) -> None:
    client = _minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


@task(retries=2, retry_delay_seconds=30)
def backup_folder(folder: str) -> dict:
    """备份单个 IMAP 文件夹，返回 {scanned, uploaded, skipped, failed}。"""
    logger = get_run_logger()

    host = _env("EMAIL_HOST", "imap.ionos.com")
    port = int(_env("EMAIL_PORT", "993") or "993")
    user = _env("EMAIL_USER")
    password = _env("EMAIL_PASSWORD")
    if not user or not password:
        raise ValueError(
            "EMAIL_USER / EMAIL_PASSWORD 未设置 —— 请在 .env 填写邮箱与密码后重试。"
        )

    bucket = _env("MINIO_BUCKET", "email-archive")
    prefix = _env("EMAIL_BACKUP_PREFIX", "email")
    client = _minio_client()

    stats = {"scanned": 0, "uploaded": 0, "skipped": 0, "failed": 0}

    imap = imaplib.IMAP4_SSL(host, port)
    try:
        imap.login(user, password)
        # readonly=True：纯只读，绝不改动服务器上的邮件
        typ, _ = imap.select(folder, readonly=True)
        if typ != "OK":
            raise RuntimeError(f"无法选择文件夹 {folder!r}: {typ}")

        typ, sdata = imap.status(folder, "(UIDVALIDITY)")
        m = re.search(rb"UIDVALIDITY (\d+)", sdata[0] if sdata else b"")
        uidvalidity = m.group(1).decode() if m else "0"

        typ, data = imap.uid("SEARCH", None, "ALL")
        uids = data[0].split() if data and data[0] else []
        logger.info("文件夹 %s：共 %d 封，UIDVALIDITY=%s", folder, len(uids), uidvalidity)

        base = f"{prefix}/{_safe(user)}/{_safe(folder)}/{uidvalidity}"
        for raw_uid in uids:
            uid = raw_uid.decode()
            stats["scanned"] += 1
            eml_key = f"{base}/{uid}.eml"
            meta_key = f"{base}/{uid}.json"
            try:
                # 幂等：对象已存在则跳过，连邮件正文都不再下载
                try:
                    client.stat_object(bucket, eml_key)
                    stats["skipped"] += 1
                    continue
                except S3Error as e:
                    if e.code not in ("NoSuchKey", "NoSuchObject"):
                        raise

                # BODY.PEEK[]：取原始 RFC822 且不置 \Seen
                typ, fdata = imap.uid("FETCH", uid, "(BODY.PEEK[])")
                if typ != "OK" or not fdata or not fdata[0]:
                    raise RuntimeError(f"FETCH 失败 uid={uid}: {typ}")
                raw = fdata[0][1]
                if not isinstance(raw, (bytes, bytearray)):
                    raise RuntimeError(f"FETCH 返回非字节 uid={uid}")

                msg = email.message_from_bytes(raw)
                sha256 = hashlib.sha256(raw).hexdigest()
                meta = {
                    "folder": folder,
                    "uidvalidity": uidvalidity,
                    "uid": uid,
                    "message_id": (msg.get("Message-ID") or "").strip(),
                    "subject": _decode(msg.get("Subject")),
                    "from": _decode(msg.get("From")),
                    "to": _decode(msg.get("To")),
                    "date": (msg.get("Date") or "").strip(),
                    "size_bytes": len(raw),
                    "sha256": sha256,
                    "backed_up_at": datetime.now(timezone.utc).isoformat(),
                }

                client.put_object(
                    bucket, eml_key, io.BytesIO(raw), length=len(raw),
                    content_type="message/rfc822",
                )
                meta_bytes = json.dumps(meta, ensure_ascii=False, indent=2).encode()
                client.put_object(
                    bucket, meta_key, io.BytesIO(meta_bytes), length=len(meta_bytes),
                    content_type="application/json",
                )
                stats["uploaded"] += 1
            except Exception as exc:  # 单封失败不致整体中断，下次幂等重试
                stats["failed"] += 1
                logger.warning("uid=%s 备份失败：%s", uid, exc)
    finally:
        try:
            imap.logout()
        except Exception:
            pass

    logger.info("文件夹 %s 完成：%s", folder, stats)
    return stats


@flow(name="email-backup")
def email_backup() -> dict:
    """把 IONOS 邮箱（默认仅 INBOX，可经 EMAIL_FOLDERS 配置）源文件备份到 MinIO。"""
    logger = get_run_logger()
    bucket = _env("MINIO_BUCKET", "email-archive")
    ensure_bucket(bucket)

    folders = [f.strip() for f in _env("EMAIL_FOLDERS", "INBOX").split(",") if f.strip()]
    if not folders:
        folders = ["INBOX"]

    total = {"scanned": 0, "uploaded": 0, "skipped": 0, "failed": 0}
    for folder in folders:
        r = backup_folder(folder)
        for k in total:
            total[k] += r[k]

    logger.info("全部完成：%s", total)
    if total["failed"]:
        # 让 Prefect UI 标红，但已成功的邮件已落 MinIO，下次幂等续传
        raise RuntimeError(f"{total['failed']} 封邮件备份失败，详见任务日志：{total}")
    return total


if __name__ == "__main__":
    email_backup()

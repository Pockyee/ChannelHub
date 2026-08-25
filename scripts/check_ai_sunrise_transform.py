#!/usr/bin/env python3
"""离线校验 orders_export.csv → ai-sunrise-DDMMYYYY.csv 的转换逻辑。

跑法（本机 python 通常没装 minio/psycopg，用 worker 镜像跑）：
    docker run --rm -v "$PWD":/w -w /w channelhub-prefect-worker \
      python scripts/check_ai_sunrise_transform.py

校验三件事：
  1) 数据行与 tests/fixtures/ai-sunrise-expected.csv 逐格一致
  2) 输出是 UTF-8 BOM + 分号分隔 + CRLF（德语 Excel 双击即开）
  3) 附件名形如 ai-sunrise-DDMMYYYY.csv
"""
import csv
import io
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "flows"))

from mail_service import (  # noqa
    OUT_HEADER, build_ai_sunrise_rows, handle_ai_sunrise_orders, is_orders_export,
)

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
SRC = os.path.join(ROOT, "tests", "fixtures", "orders_export.csv")
EXP = os.path.join(ROOT, "tests", "fixtures", "ai-sunrise-expected.csv")

payload = open(SRC, "rb").read()

failures = []


def check(cond, label, detail=""):
    print(("  ✓ " if cond else "  ✗ ") + label + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(label)


print("== 1) 表头签名识别 ==")
check(is_orders_export("orders_export.csv", payload), "识别为 Shopify 订单导出")
check(not is_orders_export("beliebig.csv", b"a,b,c\n1,2,3\n"), "无关 csv 不误判")
check(not is_orders_export("report.xlsx", payload), "非 .csv 不误判")

print("== 2) 转换结果与期望逐格比对 ==")
got = build_ai_sunrise_rows(payload)
exp_rows = list(csv.reader(io.StringIO(open(EXP, encoding="utf-8").read()), delimiter=";"))
exp_header, exp_data = exp_rows[0], [r for r in exp_rows[1:] if r]

check(OUT_HEADER == exp_header, "表头一致", f"got={OUT_HEADER}")
check(len(got) == len(exp_data), "行数一致", f"got={len(got)} expected={len(exp_data)}")

for i, (g, e) in enumerate(zip(got, exp_data), start=1):
    if g != e:
        for col, (gv, ev) in enumerate(zip(g, e)):
            if gv != ev:
                failures.append(f"row {i} col {OUT_HEADER[col]}")
                print(f"  ✗ 第 {i} 行 [{OUT_HEADER[col]}]: got={gv!r} expected={ev!r}")
if len(got) == len(exp_data) and not any(f.startswith("row ") for f in failures):
    print(f"  ✓ {len(got)} 行全部逐格一致")

print("== 3) 输出文件格式 ==")
name, data, body, n = handle_ai_sunrise_orders("orders_export.csv", payload)
check(data.startswith(b"\xef\xbb\xbf"), "带 UTF-8 BOM")
check(b";" in data.split(b"\r\n")[0], "分号分隔")
check(b"\r\n" in data, "CRLF 换行")
check(n == len(exp_data), "回信正文行数正确", f"n={n}")
check(bool(re.fullmatch(r"ai-sunrise-\d{8}\.csv", name)), "附件名格式", name)
check("ß" in data.decode("utf-8-sig"), "德语变音字符往返正确")
print(f"  · 附件名: {name}  大小: {len(data)} 字节")

print()
if failures:
    print(f"✗ {len(failures)} 项未通过")
    sys.exit(1)
print("✓ 全部通过")

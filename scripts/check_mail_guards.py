#!/usr/bin/env python3
"""离线校验邮件服务的两道护栏 + 回信自环防护。

跑法：
    docker run --rm -v "$PWD":/w -w /w channelhub-prefect-worker \
      python scripts/check_mail_guards.py

重点验证用户明确要求的规则：data@ai-sunrise.de 不回自己的信。
注意护栏**必须先于规则匹配**——ai-sunrise 规则匹配的是"发件人以 ai-sunrise.de
结尾"，而 data@ai-sunrise.de 自己也满足，没护栏就是自触发死循环。
"""
import email as email_lib
import os
import sys

os.environ.setdefault("EMAIL_USER", "data@ai-sunrise.de")
os.environ.setdefault("SMTP_USER", "data@ai-sunrise.de")

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "flows"))

import mail_service as ms          # noqa
import parse_sell_through as pst   # noqa

failures = []


def check(cond, label, detail=""):
    print(("  ✓ " if cond else "  ✗ ") + label + (f" — {detail}" if not cond else ""))
    if not cond:
        failures.append(label)


def mail(frm, subject="Bestellungen", extra_headers=""):
    return email_lib.message_from_string(
        f"From: {frm}\nTo: data@ai-sunrise.de\nSubject: {subject}\n"
        f"Message-ID: <abc@example.invalid>\n{extra_headers}\n本文\n"
    )


print("== 1) 不回自己的信（用户明确要求的规则）==")
check(ms.skip_reason(mail("data@ai-sunrise.de")) != "", "data@ai-sunrise.de 自发信被跳过")
check(ms.skip_reason(mail("DATA@AI-Sunrise.DE")) != "", "大小写变形也被跳过")
check(ms.skip_reason(mail('"ChannelHub" <data@ai-sunrise.de>')) != "", "带显示名也被跳过")
print(f"    （跳过原因：{ms.skip_reason(mail('data@ai-sunrise.de'))}）")

print("== 2) 护栏顺序：自己人的域名规则本来就会匹配到自己 ==")
check(ms.match_rule("data@ai-sunrise.de") is not None,
      "规则本身确实会匹配 data@ai-sunrise.de（所以护栏必须在前）")
check(ms.match_rule("qi.bao@ai-sunrise.de") is not None, "同事地址命中规则")
check(ms.match_rule("someone@other.de") is None, "外部域名不命中")
check(ms.match_rule("faker@evil-ai-sunrise.de") is None,
      "后缀不能被 evil-ai-sunrise.de 这类域名蒙混（必须匹配 @ai-sunrise.de）")

print("== 3) 不回自动信 ==")
check(ms.skip_reason(mail("x@ai-sunrise.de", extra_headers="Auto-Submitted: auto-replied\n")) != "",
      "Auto-Submitted 被跳过")
check(ms.skip_reason(mail("x@ai-sunrise.de", extra_headers="Precedence: bulk\n")) != "",
      "Precedence: bulk 被跳过")
check(ms.skip_reason(mail("x@ai-sunrise.de", extra_headers="List-Id: <l.example>\n")) != "",
      "邮件列表被跳过")
check(ms.skip_reason(mail("x@ai-sunrise.de")) == "", "正常同事来信**不**被跳过")

print("== 4) 回信自环：把我们自己的回信喂回两个 flow 的护栏 ==")
src = open(os.path.join(HERE, "..", "tests", "fixtures", "orders_export.csv"), "rb").read()
out_name, out_bytes, body, n = ms.handle_ai_sunrise_orders("orders_export.csv", src)
reply = ms.build_reply(mail("qi.bao@ai-sunrise.de"), "qi.bao@ai-sunrise.de",
                       "ai_sunrise_orders_export", body, (out_name, out_bytes),
                       "data@ai-sunrise.de")
check(reply["Auto-Submitted"] == "auto-replied", "回信带 Auto-Submitted")
check(reply["X-ChannelHub-Rule"] == "ai_sunrise_orders_export", "回信带 X-ChannelHub-Rule")
check(reply["In-Reply-To"] == "<abc@example.invalid>", "回信带 In-Reply-To（挂在原话题下）")
check(reply["Subject"] == "Re: Bestellungen", "回信主题加 Re:")
check(reply["To"] == "qi.bao@ai-sunrise.de", "回给原发件人，不是固定告警地址")

roundtrip = email_lib.message_from_bytes(reply.as_bytes())
check(ms.skip_reason(roundtrip) != "", "回信落回 INBOX → mail-service 跳过（不自环）")
check(pst._is_self_sent(roundtrip), "回信落回 INBOX → parse-sell-through 跳过（不误告警）")

attached = [p.get_filename() for p in roundtrip.walk() if p.get_filename()]
check(attached == [out_name], "附件名正确", str(attached))
csv_part = [p for p in roundtrip.walk() if p.get_filename() == out_name][0]
check(csv_part.get_payload(decode=True) == out_bytes, "附件字节经 SMTP 编码往返无损")

print("== 5) 回信里的附件不会被 parse-sell-through 当成订单导出入库 ==")
check(not ms.is_orders_export(out_name, out_bytes),
      "我们生成的 csv 不匹配订单导出签名（所以不会被回收入库）")

print()
if failures:
    print(f"✗ {len(failures)} 项未通过")
    sys.exit(1)
print("✓ 全部通过")

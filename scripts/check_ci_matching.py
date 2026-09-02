#!/usr/bin/env python3
"""竞品情报：型号消歧断言测试(离线，不连库、不联网)。

为什么这个测试是全项目最重要的一个：
  W2 OMNI / W2S OMNI / W2 PRO OMNI 互为前缀，HUTT 10 与 HUTT W8 同品牌。
  匹配错了**不会报错**，只会悄悄把竞品的价格和评论算到自家头上 —— 是整条流水线
  唯一「静默给出错误结论」的地方。

数据来源是**真实发布的 db/seed/ci_product.csv**(不是测试里另写一份正则)，
所以改 CSV 若破坏了消歧，这里会立刻红。

运行(与仓库现有 check_* 脚本一致，在 worker 镜像里跑)：
  docker run --rm -v "$PWD":/w -w /w channelhub-prefect-worker \
    python scripts/check_ci_matching.py

不连库、不联网：消歧读 db/seed/ci_product.csv，解析读 tests/fixtures/ci/*.html。
(需在镜像里跑是因为 ci_price 依赖 prefect;ci_common 本身只用标准库。)
"""

import csv
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "flows"))

from ci_common import (
    Product,
    is_accessory,
    looks_like_bot_wall,
    match_product,
    match_products_all,
)
from ci_price import (
    _eur_to_cents,
    parse_amazon,
    parse_geizhals,
    parse_jsonld_product,
    parse_mydealz_rss,
)

REPO = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")

failures = []


def check(cond, label, detail=""):
    if cond:
        print(f"  ✓ {label}")
    else:
        print(f"  ✗ {label}" + (f"  — {detail}" if detail else ""))
        failures.append(label)


def load_seed_products():
    """直接读发布用的 seed CSV —— 测的是真正会上线的那份正则。"""
    path = os.path.join(REPO, "db", "seed", "ci_product.csv")
    out = []
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            out.append(Product(
                product_id=r["product_id"].strip(),
                brand=r["brand"].strip(),
                display_name=r["display_name"].strip(),
                is_own=r["is_own"].strip().lower() in ("true", "t", "1", "yes"),
                ean=(r["ean"].strip() or None),
                brand_re=re.compile(r["brand_regex"], re.IGNORECASE) if r["brand_regex"].strip() else None,
                model_re=re.compile(r["match_regex"], re.IGNORECASE) if r["match_regex"].strip() else None,
            ))
    return out


PRODUCTS = load_seed_products()

# ---------------------------------------------------------------------------
print("1) seed CSV 完整性")
check(len(PRODUCTS) == 5, f"5 款产品(读到 {len(PRODUCTS)})")
check(sum(1 for p in PRODUCTS if p.is_own) == 1, "恰好一款 is_own")
check(all(p.brand_re and p.model_re for p in PRODUCTS), "每款都有 brand_regex 与 match_regex")

# ---------------------------------------------------------------------------
print("2) 单品语境(商品页/报价)：正确命中")
POSITIVE = [
    ("ECOVACS WINBOT W3 OMNI Fensterreinigungsroboter",              "ecovacs-w3-omni"),
    ("Ecovacs Winbot W3 Omni, weiß",                                 "ecovacs-w3-omni"),
    ("ECOVACS WINBOT W2S OMNI Fensterputzroboter",                   "ecovacs-w2s-omni"),
    ("Winbot W2S Omni",                                              "ecovacs-w2s-omni"),
    ("ECOVACS WINBOT W2 PRO OMNI",                                   "ecovacs-w2-pro-omni"),
    ("Ecovacs Winbot W2 Pro Omni Fensterroboter",                    "ecovacs-w2-pro-omni"),
    ("HUTT 10 Fensterputzroboter Weiß",                              "hutt-10"),
    ("HUTT Fensterputzroboter 10 weiß eckig",                        "hutt-10"),
    ("Hutt 10 Fensterreinigungsroboter",                             "hutt-10"),
    ("ECOVACS Winbot Mini Fensterreinigungsroboter grau",             "ecovacs-winbot-mini"),
    ("Ecovacs Winbot Mini, grau",                                     "ecovacs-winbot-mini"),
]
for title, expect in POSITIVE:
    got, reason = match_product(title, PRODUCTS)
    check(got == expect, f"{title!r} → {expect}", f"实得 {got} ({reason})")

# ---------------------------------------------------------------------------
print("3) 单品语境：**必须不命中**(前缀陷阱与他型号)")
NEGATIVE = [
    # W2 OMNI 是另一款，不在跟踪范围 —— 绝不能被 W2 PRO 或 W2S 的正则吃掉
    "ECOVACS WINBOT W2 OMNI Fensterputzroboter",
    "Ecovacs Winbot W2 Omni",
    # HUTT 另有型号
    "HUTT W8 Fensterputzroboter",
    "HUTT W55 Fensterreiniger",
    # 数字粘连
    "ECOVACS WINBOT W30 Testgerät",
    "HUTT 100 Industriereiniger",
    # MINI 不得与 W 系列互串（品牌闸门 + 型号 token 都要成立）
    "Kärcher Mini Fensterreiniger",
    "HUTT Mini Fensterputzroboter",
    # 完全无关
    "Kärcher WV 6 Plus Fenstersauger",
    "",
]
for title in NEGATIVE:
    got, reason = match_product(title, PRODUCTS)
    check(got is None, f"{title!r} → 不命中", f"误配成 {got} ({reason})")

# ---------------------------------------------------------------------------
print("4) 配件/耗材必须排除(否则会被当成整机算进销量)")
ACCESSORIES = [
    "ECOVACS WINBOT W2 PRO OMNI Ersatztücher 6er Set",
    "HUTT 10 Ersatztücher 10 Stück",
    "Winbot W3 Omni Reinigungsmittel Nachfüllpack",
    "Ecovacs Winbot W2S Omni Sicherheitsseil Zubehör",
]
for title in ACCESSORIES:
    check(is_accessory(title.lower()), f"{title!r} 判为配件")
    got, _ = match_product(title, PRODUCTS)
    check(got is None, f"{title!r} → 不计为整机", f"误配成 {got}")

# ---------------------------------------------------------------------------
print("5) 歧义：单品语境判失败，文档语境全收")
AMBIGUOUS = "Vergleich: Ecovacs Winbot W2S Omni gegen Winbot W3 Omni im Test"
got, reason = match_product(AMBIGUOUS, PRODUCTS)
check(got is None, "对比标题在单品语境判失败(不猜)", f"实得 {got}")
check(reason.startswith("ambiguous"), f"失败原因标为 ambiguous(实得 {reason!r})")

all_hits = match_products_all(AMBIGUOUS, PRODUCTS)
check(set(all_hits) == {"ecovacs-w2s-omni", "ecovacs-w3-omni"},
      "对比标题在文档语境同时命中两款", f"实得 {all_hits}")

CROSS = "HUTT 10 vs. ECOVACS Winbot W3 Omni — welcher Fensterputzroboter lohnt sich?"
check(set(match_products_all(CROSS, PRODUCTS)) == {"hutt-10", "ecovacs-w3-omni"},
      "自家 vs 竞品对比文同时挂两款", f"实得 {match_products_all(CROSS, PRODUCTS)}")

# ---------------------------------------------------------------------------
print("6) 品牌闸门：型号 token 不得脱离品牌单独命中")
check(match_product("Modell W3 Reinigungsgerät ohne Marke", PRODUCTS)[0] is None,
      "无品牌的 'W3' 不命中")
check(match_product("Angebot: 10 Stück Mikrofasertücher", PRODUCTS)[0] is None,
      "无品牌的 '10' 不命中 HUTT 10")

# ---------------------------------------------------------------------------
print("7) 金额解析(德式/英式千分位都要吃下)")
for raw, want in [("499.00", 49900), ("479,99", 47999), ("1.234,56", 123456),
                  ("1,234.56", 123456), ("389,99&euro;", 38999), ("299 €", 29900),
                  ("", None), ("k.A.", None)]:
    check(_eur_to_cents(raw) == want, f"_eur_to_cents({raw!r}) == {want}",
          f"实得 {_eur_to_cents(raw)}")

# ---------------------------------------------------------------------------
print("8) JSON-LD 通用抽取(idealo/Geizhals/MMS/Otto 共用这一份)")
def fx(name):
    with open(os.path.join(REPO, "tests", "fixtures", "ci", name), encoding="utf-8") as f:
        return f.read()

d = parse_jsonld_product(fx("jsonld_pricecompare.html"))
check(d["name"] and "W2 PRO" in d["name"].upper(), "取到商品名")
check(len(d["offers"]) == 3, f"取到 3 条 Offer(实得 {len(d['offers'])})")
check({o["merchant"] for o in d["offers"]} == {"Otto", "MediaMarkt"}, "商家名正确")
check(min(o["price_cents"] for o in d["offers"]) == 47999, "最低价 479,99 → 47999")
check(d["review_count"] == 1287, f"评论数 1.287 → 1287(实得 {d['review_count']})")
check(abs((d["rating"] or 0) - 4.4) < 1e-6, f"评分 4,4 → 4.4(实得 {d['rating']})")

d2 = parse_jsonld_product(fx("jsonld_graph_aggregate.html"))
check(d2["name"] == "Hutt 10 Fensterreinigungsroboter", "@graph 里的 Product 能取到")
check(len(d2["offers"]) == 2, f"AggregateOffer 的 low/high 都取(实得 {len(d2['offers'])})")
check(min(o["price_cents"] for o in d2["offers"]) == 29100, "lowPrice 291,00 → 29100")
check(parse_jsonld_product("<html><body>无结构化数据</body></html>")["offers"] == [],
      "无 JSON-LD 时安全返回空(不抛异常)")

# ---------------------------------------------------------------------------
print("9) Amazon 解析：BSR 是全项目唯一的销量信号")
a = parse_amazon(fx("amazon_dp.html"))
check(a["name"] and "W2S OMNI" in a["name"].upper(), f"标题(实得 {a['name']!r})")
check(a["price_cents"] == 38999, f"价格 389,99 → 38999(实得 {a['price_cents']})")
check(abs((a["rating"] or 0) - 4.3) < 1e-6, f"评分 4,3(实得 {a['rating']})")
check(a["review_count"] == 2143, f"评论数 2.143 → 2143(实得 {a['review_count']})")
check(a["bsr_rank"] == 1842, f"BSR 名次 1.842 → 1842(实得 {a['bsr_rank']})")
check(a["bsr_category"] == "Baumarkt", f"BSR 类目(实得 {a['bsr_category']!r})")
# 解析出的标题必须能反查回正确产品 —— 抓取与消歧的接缝处
check(match_product(a["name"], PRODUCTS)[0] == "ecovacs-w2s-omni", "Amazon 标题反查回正确 SKU")

# ---------------------------------------------------------------------------
print("9b) Geizhals 专用解析（该站不发 JSON-LD，通用路径在这里不成立）")
g = parse_geizhals(fx("geizhals_offers.html"))
check(parse_jsonld_product(fx("geizhals_offers.html"))["offers"] == [],
      "确认 Geizhals 页面确实没有 JSON-LD（所以才需要专用解析）")
check(len(g["offers"]) >= 3, f"解析出 >=3 条报价(实得 {len(g['offers'])})")
check(all(o["price_cents"] and o["price_cents"] > 0 for o in g["offers"]),
      "每条报价都有正价格")
check(all(o["merchant"] for o in g["offers"]), "每条报价都有商家名")
# 一个 offer 块里有多个 data-merchant-name（按钮 + 商家 logo），
# 全局匹配会把价格和商家配错对 —— 这里锁死「按块切分」的正确性
merchants = [o["merchant"] for o in g["offers"]]
check(len(merchants) == len(g["offers"]), "商家数与报价数一一对应（未因全局匹配串位）")
check(g["name"] and "Hutt 10" in g["name"], f"取到商品名(实得 {g['name']!r})")
check(min(o["price_cents"] for o in g["offers"]) == 24990,
      f"最低价 € 249,90 → 24990(实得 {min(o['price_cents'] for o in g['offers'])})")
check(parse_geizhals("<html><body>leer</body></html>")["offers"] == [],
      "空页面安全返回空（不抛异常）")

# ---------------------------------------------------------------------------
print("9c) mydealz RSS（公开 feed，无需 REST 签名）")
with open(os.path.join(REPO, "tests", "fixtures", "ci", "mydealz_group.xml"), "rb") as _f:
    raw = _f.read()
items = parse_mydealz_rss(raw)
check(len(items) >= 5, f"解析出 >=5 条 item(实得 {len(items)})")
check(all(i["title"] and i["link"] for i in items), "每条都有标题与链接")
priced = [i for i in items if i["price_cents"]]
check(len(priced) >= 5, f"pepper:merchant 的 name/price 都取到(实得 {len(priced)})")
check(any(i["merchant"] == "eBay" and i["price_cents"] == 44800 for i in items),
      "448€ → 44800 且商家 eBay")
check(any(i["price_cents"] == 42842 for i in items), "德式 428,42€ → 42842")

# 关键反例：/rss/gruppe/ecovacs 里绝大多数是 **Deebot 扫地机**。品牌闸门会放行，
# 必须靠型号正则挡住 —— 否则扫地机的促销价会被算成擦窗机的竞品价。
matched = [(i["title"][:45], match_product(i["title"], PRODUCTS)[0]) for i in items]
deebots = [t for t, pid in matched if "Deebot" in t or "T90" in t or "T80" in t or "X12" in t]
check(all(pid is None for t, pid in matched
          if "Deebot" in t or "T90" in t or "T80" in t or "X12" in t),
      f"ECOVACS Deebot/T90/T80/X12 一律不命中擦窗机({len(deebots)} 条反例)")
check(any(pid == "ecovacs-w3-omni" for _, pid in matched), "WINBOT W3 条目正确命中")

# ---------------------------------------------------------------------------
print("10) 反爬墙必须显式失败(不得静默存空记录)")
# idealo 用 Akamai Bot Manager：HTTP **200** 返回 2.6KB JS 挑战壳，
# 既无验证码字样也无错误码。认不出来就会被当成「今天没数据」，在价格曲线上留假断点。
check(looks_like_bot_wall(fx("akamai_challenge.html"), 200),
      "Akamai JS 挑战页（HTTP 200）判为反爬墙")
check(not looks_like_bot_wall(fx("geizhals_offers.html"), 200),
      "正常 Geizhals 报价页不误判")
check(looks_like_bot_wall(fx("amazon_botwall.html"), 200), "验证码页在 200 下也判为反爬墙")
check(looks_like_bot_wall("", 503), "503 判为反爬墙")
check(looks_like_bot_wall("", 429), "429 判为反爬墙")
check(not looks_like_bot_wall(fx("amazon_dp.html"), 200), "正常商品页不误判")

# ---------------------------------------------------------------------------
print()
if failures:
    print(f"✗ {len(failures)} 项失败：")
    for f in failures:
        print(f"   - {f}")
    sys.exit(1)
print("✓ 全部通过。")

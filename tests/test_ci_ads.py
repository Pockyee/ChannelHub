"""flows/ci_ads.py 的纯函数测试（不连网、不连库）。

运行（worker 镜像里依赖齐全）:
  docker run --rm -v "$PWD:/repo" -w /repo channelhub-prefect-worker python tests/test_ci_ads.py
也兼容 pytest。
"""
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "flows"))
FIX = ROOT / "tests" / "fixtures" / "ci"

import ci_ads  # noqa: E402
from ci_common import Product  # noqa: E402

TODAY = date(2026, 9, 28)
PRODUCTS = [
    Product("hutt-10", "HUTT", "HUTT 10", True, None,
            re.compile(r"\bhutt\b", re.I), re.compile(r"\b(?:w\s*)?10\b", re.I)),
    Product("ecovacs-w3-omni", "ECOVACS", "ECOVACS WINBOT W3 OMNI", False, None,
            re.compile(r"\b(?:ecovacs|winbot)\b", re.I), re.compile(r"\bw3\b", re.I)),
]


def _meta():
    return json.loads((FIX / "meta_ads_archive.json").read_text())


def _schema():
    return ci_ads.flatten_schema(json.loads((FIX / "bq_creative_stats_schema.json").read_text())
                                 ["schema"]["fields"])


def test_parse_meta_running_ad():
    ad = _meta()["data"][0]
    row = ci_ads.parse_meta_ad(ad, "ECOVACS", PRODUCTS, "run1", today=TODAY)
    assert row["source_code"] == "meta_ads" and row["ad_id"] == "1234567890123456"
    assert row["advertiser_name"] == "ECOVACS Robotics DE"
    assert row["surfaces"] == ["facebook", "instagram"]
    assert row["first_shown"] == date(2026, 9, 2)
    assert row["is_active"] is True and row["last_shown"] == TODAY
    assert row["exposure_lower"] == row["exposure_upper"] == 48210
    assert row["exposure_kind"] == "reach"
    assert row["product_ids"] == ["ecovacs-w3-omni"]
    assert row["body"].count("WINBOT W3 OMNI putzt") == 1          # 重复卡片文案去重
    assert row["targeting"]["gender"] == "All"
    # 硬约束：入库的链接绝不能带 token
    assert row["ad_url"] == "https://www.facebook.com/ads/library/?id=1234567890123456"
    assert "token" not in json.dumps(row, default=str)


def test_parse_meta_ended_ad_uses_stop_date():
    ad = _meta()["data"][2]
    row = ci_ads.parse_meta_ad(ad, "ECOVACS", PRODUCTS, "run1", today=TODAY)
    assert row["is_active"] is False
    assert row["first_shown"] == date(2026, 7, 1) and row["last_shown"] == date(2026, 7, 20)
    assert row["exposure_kind"] is None and row["product_ids"] == []


def test_keyword_mode_filters_resellers_by_page_name():
    brand_re = ci_ads.brand_targets(PRODUCTS)["ECOVACS"]
    names = [a["page_name"] for a in _meta()["data"]]
    kept = [n for n in names if brand_re.search(n)]
    assert "Robot-Shop24" not in kept and kept.count("ECOVACS Robotics DE") == 2


def test_brand_targets_fallback_regex():
    p = Product("x", "Hobot", "Hobot 2S", False, None, None, None)
    assert ci_ads.brand_targets([p])["Hobot"].search("HOBOT Technology")


def test_meta_error_kind():
    assert ci_ads.meta_error_kind({"code": 190}) == "auth_expired"
    assert ci_ads.meta_error_kind({"code": 613}) == "rate_limited"
    assert ci_ads.meta_error_kind({"code": 10}) == "permission"
    assert ci_ads.meta_error_kind({"code": 200}) == "permission"
    assert ci_ads.meta_error_kind({"code": 100}) == "error"


def test_resolve_columns_and_surfaces():
    schema = _schema()
    cols = ci_ads.resolve_columns(schema)
    assert cols["creative_id"] == "creative_id"
    assert cols["advertiser_name"] == "advertiser_disclosed_name"
    # 真实字段名是 .surface(2026-09 实测),不是早期资料里的 surface_code
    assert cols["surface_code"] == "region_stats.surface_serving_stats.surface_serving_stats.surface"
    expr = ci_ads._surface_expr(cols["surface_code"], schema)
    assert expr == ("ARRAY(SELECT SAFE_CAST(s.surface AS STRING) "
                    "FROM UNNEST(r.surface_serving_stats.surface_serving_stats) AS s)")


def test_resolve_columns_accepts_ad_id_alias():
    schema = {k.replace("creative_id", "ad_id"): v for k, v in _schema().items()}
    assert ci_ads.resolve_columns(schema)["creative_id"] == "ad_id"


def test_resolve_columns_missing_required_raises():
    schema = {k: v for k, v in _schema().items() if k != "creative_id"}
    try:
        ci_ads.resolve_columns(schema)
    except ci_ads.SchemaChanged as e:
        assert "creative_id" in str(e)
    else:
        raise AssertionError("缺必需列应抛 SchemaChanged")


def test_build_google_sql_ids_and_regex():
    schema = _schema()
    cols = ci_ads.resolve_columns(schema)
    sql, params = ci_ads.build_google_sql(cols, schema, [
        ("ECOVACS", "ids", ["AR1", "AR2"]),
        ("HUTT", "regex", r"\bhutt\b", set()),
    ])
    assert "FROM `bigquery-public-data.google_ads_transparency_center.creative_stats` AS c, " \
           "UNNEST(c.region_stats) AS r" in sql
    assert "c.advertiser_id IN UNNEST(@ids_0)" in sql
    assert "REGEXP_CONTAINS(c.advertiser_disclosed_name, @re_1)" in sql
    assert "r.region_code = @region" in sql
    assert "SAFE_CAST(r.last_shown AS DATE) >= @since" in sql
    by_name = {p["name"]: p for p in params}
    assert by_name["re_1"]["parameterValue"]["value"] == r"(?i)\bhutt\b"
    assert [v["value"] for v in by_name["ids_0"]["parameterValue"]["arrayValues"]] == ["AR1", "AR2"]
    assert by_name["region"]["parameterValue"]["value"] == "DE"


def test_build_google_sql_excludes_blocked_advertisers():
    schema = _schema()
    cols = ci_ads.resolve_columns(schema)
    sql, params = ci_ads.build_google_sql(cols, schema, [
        ("HUTT", "regex", r"\bhutt\b", {"AR_GALLERY", "AR_TSHIRT"})])
    assert "(REGEXP_CONTAINS(c.advertiser_disclosed_name, @re_0) AND " \
           "c.advertiser_id NOT IN UNNEST(@ex_0))" in sql
    ex = next(p for p in params if p["name"] == "ex_0")
    assert [v["value"] for v in ex["parameterValue"]["arrayValues"]] == ["AR_GALLERY", "AR_TSHIRT"]


def test_google_brand_specs_allow_deny_optout():
    brands = ci_ads.brand_targets(PRODUCTS)
    allow = {("google_ads", "ECOVACS"): ["AR_ECO"]}
    # HUTT 禁用按名检索 → 不出现在查询里
    deny = {("google_ads", "HUTT"): {ci_ads.OPT_OUT}}
    specs = ci_ads.google_brand_specs(brands, allow, deny)
    assert specs == [("ECOVACS", "ids", ["AR_ECO"])]
    # 只排除个别同名公司 → 仍按名检索,带排除集
    deny = {("google_ads", "HUTT"): {"AR_GALLERY"}}
    specs = ci_ads.google_brand_specs(brands, {}, deny)
    assert ("HUTT", "regex", r"\bhutt\b", {"AR_GALLERY"}) in specs


def test_parse_bq_rows_and_google_row():
    resp = json.loads((FIX / "bq_query_response.json").read_text())
    recs = ci_ads.parse_bq_rows(resp)
    assert recs[0]["surfaces"] == ["YOUTUBE", "SEARCH"]
    r0 = ci_ads.google_row(recs[0], "run1", "DE", today=TODAY)
    assert r0["is_active"] is True                                   # 2 天前还在展示
    assert (r0["exposure_lower"], r0["exposure_upper"], r0["exposure_kind"]) == (10000, 15000, "impressions")
    assert r0["surfaces"] == ["SEARCH", "YOUTUBE"]
    assert r0["ad_url"].startswith("https://adstransparency.google.com/advertiser/AR01234567890123456789/")
    assert r0["targeting"]["demographic_info"] == "Included"
    r1 = ci_ads.google_row(recs[1], "run1", "DE", today=TODAY)
    assert r1["is_active"] is False and r1["exposure_kind"] is None
    assert r1["ad_url"].endswith("/creative/CR22222222222222222222")


def test_write_ads_dry_run_dedupes():
    os.environ["CI_DRY_RUN"] = "true"
    ad = _meta()["data"][0]
    row = ci_ads.parse_meta_ad(ad, "ECOVACS", PRODUCTS, "run1", today=TODAY)
    assert ci_ads._write_ads([row, dict(row)]) == 1


# ---------------------------------------------------------------------------
# 按落地页域名归属（021）
# ---------------------------------------------------------------------------
class _Log:
    def info(self, *a): pass
    def warning(self, *a): pass


def _tc_item(adv, cid):
    return {"1": adv, "2": cid, "3": {"1": {"4": "https://…"}}, "12": "BlueVision Interactive Limited"}


def test_tc_request_region_code_and_token():
    req = ci_ads.tc_request("imoostore.com", "DE")
    assert req["3"] == {"8": [2276], "12": {"1": "imoostore.com", "2": True}}
    assert req["7"]["3"] == 2276 and "4" not in req
    assert ci_ads.tc_request("imoo.me", "FR", "tok")["4"] == "tok"
    try:
        ci_ads.tc_request("imoo.me", "US")
        assert False, "非 EEA 地区应拒绝"
    except ci_ads.DomainLookupFailed:
        pass


def test_parse_tc_page_ok_and_changed_shape():
    items, token, total = ci_ads.parse_tc_page(
        {"1": [_tc_item("AR1", "CR1"), _tc_item("AR1", "CR2")], "2": "next", "4": "93", "5": "x"})
    assert items == {"CR1": "AR1", "CR2": "AR1"} and token == "next" and total == 93
    assert ci_ads.parse_tc_page({"4": "0"}) == ({}, None, 0)             # 域名没有广告
    for bad in ([], {"1": "oops"}, {"1": [{"1": 123, "2": "CR1"}]}):
        try:
            ci_ads.parse_tc_page(bad)
            assert False, f"结构变了应抛: {bad}"
        except ci_ads.DomainLookupFailed:
            pass


def test_tc_domain_creatives_paginates_and_checks_total():
    ci_ads.TC_PAGE_PAUSE = 0
    pages = {None: {"1": [_tc_item("AR1", "CR1"), _tc_item("AR1", "CR2")], "2": "p2", "4": "3"},
             "p2": {"1": [_tc_item("AR2", "CR3")], "4": "3"}}
    seen = []

    def post(req):
        seen.append(req.get("4"))
        return pages[req.get("4")]
    got = ci_ads.tc_domain_creatives("imoostore.com", "DE", post=post)
    assert got == {"CR1": "AR1", "CR2": "AR1", "CR3": "AR2"} and seen == [None, "p2"]
    # 报告 5 条只取到 3 条 → 不完整，按失败处理
    pages["p2"]["4"] = "5"
    try:
        ci_ads.tc_domain_creatives("imoostore.com", "DE", post=post)
        assert False, "条数对不上应抛"
    except ci_ads.DomainLookupFailed:
        pass
    # 改版后照样 200 但字段全没了 → 失败，不是「0 条广告」
    try:
        ci_ads.tc_domain_creatives("imoostore.com", "DE", post=lambda req: {"9": "new"})
        assert False, "空响应应抛"
    except ci_ads.DomainLookupFailed:
        pass
    # 明确报告 0 条 → 确实没有广告
    assert ci_ads.tc_domain_creatives("imoostore.com", "DE", post=lambda req: {"4": "0"}) == {}


def test_resolve_domain_creatives_zero_with_known_is_failure():
    use, prunable, failures = ci_ads.resolve_domain_creatives(
        {"imoo": ["imoostore.com"]}, "DE", _Log(),
        lookup=lambda d, r: {}, known=lambda src, brand: {"CR_OLD"})
    # 原本有 93 条、突然 0 条 → 疑似改版：沿用旧清单、不删、要告警
    assert use["imoo"] == {"CR_OLD"} and "imoo" not in prunable and len(failures) == 1
    # 库里本来就没有 → 真的是 0 条，照常
    use, prunable, failures = ci_ads.resolve_domain_creatives(
        {"imoo": ["imoostore.com"]}, "DE", _Log(),
        lookup=lambda d, r: {}, known=lambda src, brand: set())
    assert use["imoo"] == set() and prunable["imoo"] == set() and not failures


def test_resolve_domain_creatives_falls_back_without_pruning():
    def lookup(domain, region):
        if domain == "imoo.me":
            raise ci_ads.DomainLookupFailed("HTTP 500")
        return {"CR_NEW": "AR_BV"}
    known = lambda src, brand: {"CR_OLD"}                                  # noqa: E731
    use, prunable, failures = ci_ads.resolve_domain_creatives(
        {"imoo": ["imoo.me", "imoostore.com"], "Xplora": ["xplora.de"]}, "DE", _Log(),
        lookup=lookup, known=known)
    # imoo 有一个域名失败：沿用已知 ∪ 本轮成功的，但不能据此删行
    assert use["imoo"] == {"CR_OLD", "CR_NEW"} and "imoo" not in prunable
    assert use["Xplora"] == {"CR_NEW"} and prunable["Xplora"] == {"CR_NEW"}
    assert len(failures) == 1 and failures[0].startswith("imoo / imoo.me")


def test_google_brand_specs_domain_replaces_name_search():
    products = PRODUCTS + [Product("imoo-z7", "imoo", "imoo Z7", True, None,
                                   re.compile(r"\bimoo\b", re.I), None)]
    brands = ci_ads.brand_targets(products)
    allow = {("google_ads", "ECOVACS"): ["AR_ECO"]}
    specs = ci_ads.google_brand_specs(brands, allow, {}, {"imoo": {"CR1", "CR2"}, "ECOVACS": {"CR9"}})
    assert ("imoo", "creatives", {"CR1", "CR2"}) in specs
    assert not any(s[0] == "imoo" and s[1] == "regex" for s in specs)    # 不再按名检索
    # 白名单账户与域名素材并用
    assert ("ECOVACS", "ids", ["AR_ECO"]) in specs and ("ECOVACS", "creatives", {"CR9"}) in specs
    assert ("HUTT", "regex", r"\bhutt\b", set()) in specs                 # 其它品牌不受影响
    # 登记了域名但一条素材都没有 → 什么都不查（也不回退到按名检索）
    specs = ci_ads.google_brand_specs(brands, {}, {}, {"imoo": set()})
    assert not any(s[0] == "imoo" for s in specs)


def test_build_google_sql_creatives():
    schema = _schema()
    cols = ci_ads.resolve_columns(schema)
    sql, params = ci_ads.build_google_sql(cols, schema, [("imoo", "creatives", {"CR2", "CR1"})])
    assert "c.creative_id IN UNNEST(@cr_0)" in sql
    cr = next(p for p in params if p["name"] == "cr_0")
    assert [v["value"] for v in cr["parameterValue"]["arrayValues"]] == ["CR1", "CR2"]


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {name}: {type(e).__name__}: {e}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)

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

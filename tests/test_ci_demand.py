"""flows/ci_demand.py 的解析函数 + flows/ci_price.py 的 Amazon 解析测试（不连网、不连库）。

运行（worker 镜像里依赖齐全）:
  docker run --rm -v "$PWD:/repo" -w /repo channelhub-prefect-worker python tests/test_ci_demand.py
也兼容 pytest。
"""
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "flows"))

import ci_demand  # noqa: E402
import ci_price  # noqa: E402

TERMS = ["imoo", "xplora", "kinder smartwatch"]


# ---------------------------------------------------------------------------
# Google Trends
# ---------------------------------------------------------------------------
def test_trends_explore_picks_timeseries_widget():
    body = ")]}'\n" + json.dumps({"widgets": [
        {"id": "GEO_MAP", "request": {}, "token": "x"},
        {"id": "TIMESERIES", "request": {"time": "2021-09-29 2026-09-29"}, "token": "tok-1"},
    ]})
    req, token = ci_demand.parse_trends_explore(body)
    assert token == "tok-1" and req["time"].startswith("2021")


def test_trends_multiline_rows_per_week_and_term():
    body = ")]}',\n" + json.dumps({"default": {"timelineData": [
        {"time": "1632614400", "value": [0, 32, 27], "hasData": [False, True, True]},
        {"time": "1790467200", "value": [6, 61, 28], "hasData": [True, True, True],
         "isPartial": True},
    ]}})
    rows = ci_demand.parse_trends_multiline(body, TERMS)
    assert len(rows) == 6
    first = rows[0]
    assert first["term"] == "imoo" and first["value"] == 0 and first["has_data"] is False
    assert first["period_start"] == date(2021, 9, 26) and first["is_partial"] is False
    last = rows[-1]
    assert last["term"] == "kinder smartwatch" and last["value"] == 28 and last["is_partial"] is True


def test_trends_multiline_rejects_term_count_mismatch():
    body = json.dumps({"default": {"timelineData": [{"time": "1632614400", "value": [1, 2]}]}})
    try:
        ci_demand.parse_trends_multiline(body, TERMS)
    except ValueError:
        return
    raise AssertionError("值个数与词数不一致时必须报错，不能错位入库")


# ---------------------------------------------------------------------------
# Google Play 详情页
# ---------------------------------------------------------------------------
def _play_page(ds5) -> str:
    return ("<html><script>AF_initDataCallback({key: 'ds:1', hash: '1', data:[1], sideChannel: {}});"
            "</script><script>AF_initDataCallback({key: 'ds:5', hash: '2', data:"
            + json.dumps(ds5) + ", sideChannel: {}});</script></html>")


def _ds5():
    b = [None] * 146
    b[0] = ["imoo Watch Phone"]
    b[13] = ["1.000.000+", 1000000, 2704927, "1\xa0Mio.+"]
    b[51] = [[None, 2.6143], None, [None, 13669], [None, 4321]]
    b[140] = [[["9.36.80"]]]
    b[145] = [[None, [1790494994, 0]]]
    return [None, [None, None, b]]


def test_play_details_extracts_installs_and_ratings():
    d = ci_demand.parse_play_details(_play_page(_ds5()))
    assert d["title"] == "imoo Watch Phone"
    assert d["installs_text"] == "1.000.000+" and d["installs_min"] == 1000000
    assert d["installs_exact"] == 2704927
    assert d["rating_avg"] == 2.614 and d["rating_count"] == 13669 and d["review_count"] == 4321
    assert d["app_version"] == "9.36.80" and isinstance(d["app_updated_on"], date)


def test_play_details_missing_block_is_all_none():
    d = ci_demand.parse_play_details("<html>kein ds:5</html>")
    assert all(v is None for v in d.values())


def test_installs_min_parses_german_and_english_thousands():
    assert ci_demand._installs_min("500.000+") == 500000
    assert ci_demand._installs_min("1,000,000+") == 1000000
    assert ci_demand._installs_min(None) is None


# ---------------------------------------------------------------------------
# App Store (iTunes lookup)
# ---------------------------------------------------------------------------
def test_itunes_lookup():
    d = ci_demand.parse_itunes_lookup({"resultCount": 1, "results": [{
        "trackName": "Xplora", "averageUserRating": 4.23723, "userRatingCount": 38715,
        "version": "1.2.9", "currentVersionReleaseDate": "2026-09-25T08:46:35Z"}]})
    assert d["rating_avg"] == 4.237 and d["rating_count"] == 38715
    assert d["app_updated_on"] == date(2026, 9, 25)
    assert ci_demand.parse_itunes_lookup({"resultCount": 0, "results": []}) is None


# ---------------------------------------------------------------------------
# Amazon：评分只认商品自己的 acrPopover，价格认字面「€」
# ---------------------------------------------------------------------------
AMAZON_PAGE = """
<span id="productTitle" class="a-size-large"> imoo X10 Smartwatch Kinder </span>
<div class="a-row"><span>von der Assurant Europe Insurance N.V.</span></div>
<a aria-label="1,0 von 5 Sternen 1 Kundenrezensionen" href="#"></a>
<span id="acrPopover" class="reviewCountTextLinkedHistogram noUnderline" title="4,8 von 5 Sternen">
<span id="acrCustomerReviewText">7 Sternebewertungen</span>
<span class="a-price apex-pricetopay-value"><span class="a-offscreen">299,00€</span></span>
<script>{"priceAmount": 299.00}</script>
<li>Amazon Bestseller-Rang: Nr. 3.313 in Elektronik & Foto (Siehe Top 100)</li>
"""


def test_amazon_rating_uses_product_popover_not_first_star_widget():
    d = ci_price.parse_amazon(AMAZON_PAGE)
    assert d["rating"] == 4.8
    assert d["review_count"] == 7
    assert d["price_cents"] == 29900
    assert d["bsr_rank"] == 3313 and d["bsr_category"].startswith("Elektronik")


def test_amazon_price_falls_back_to_offscreen_with_literal_euro():
    page = AMAZON_PAGE.replace('{"priceAmount": 299.00}', "")
    assert ci_price.parse_amazon(page)["price_cents"] == 29900


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"✓ {name}")
            except Exception as e:           # noqa: BLE001
                fails += 1
                print(f"✗ {name}: {e!r}")
    sys.exit(1 if fails else 0)

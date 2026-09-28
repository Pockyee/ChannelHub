"""flows/ci_price.py 里 mydealz 相关纯函数的测试（不连网、不连库）。

运行（worker 镜像里依赖齐全）:
  docker run --rm -v "$PWD:/repo" -w /repo channelhub-prefect-worker python tests/test_ci_mydealz.py
也兼容 pytest。
"""
import html
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "flows"))

import ci_price  # noqa: E402


def _card(thread: dict) -> str:
    """仿真实页面：单引号属性 + HTML 转义过的 JSON。"""
    payload = json.dumps({"name": "ThreadMainListItemNormalizer", "props": {"thread": thread}})
    return f"<div class=\"js-vue3\" data-vue3='{html.escape(payload, quote=True)}'></div>"


PAGE = "".join([
    _card({"threadId": "2827699", "type": "Deal",
           "titleSlug": "telekom-smart-connect-kids-watchgps-tracker-inkl-smartwatch-xplora-x6-playanio-6",
           "title": "Telekom Smart Connect (Kids Watch/GPS-Tracker) inkl. Smartwatch "
                    "(Xplora X6 Play/Anio 6) für eff. 6,46 €/Monat | 1 GB & Allnet-Flat [Nicotel]",
           "publishedAt": 1787218390, "price": 154.95,
           "merchant": {"merchantName": "Nicotel"}}),
    _card({"threadId": "2800001", "type": "Discussion", "titleSlug": "frage-kinderuhr",
           "title": "Welche Kinderuhr?", "publishedAt": 1787000000}),
    '<div data-vue3=\'{"name":"SomethingElse","props":{}}\'></div>',
    '<script>{"pagination":{"currentPage":1,"lastPage":3}}</script>',
])


def test_listing_parses_deal_cards_only():
    items, last = ci_price.parse_mydealz_listing(PAGE)
    assert last == 3
    assert len(items) == 1                                   # 讨论帖与其他挂载点被跳过
    it = items[0]
    assert it["link"] == ("https://www.mydealz.de/deals/telekom-smart-connect-kids-watchgps-"
                          "tracker-inkl-smartwatch-xplora-x6-playanio-6-2827699")
    assert it["guid"] == it["link"]                          # 与 RSS 路径同一 external_id
    assert "Xplora X6 Play" in it["title"]                   # 转义还原(€ / ü / &)
    assert it["price_cents"] == 15495 and it["merchant"] == "Nicotel"
    assert it["published"] == 1787218390


def test_listing_without_pagination_defaults_to_one_page():
    assert ci_price.parse_mydealz_listing("<html></html>") == ([], 1)


def test_contract_deals_detected():
    for t in ("Telekom Smart Connect (Kids Watch/GPS-Tracker) inkl. Smartwatch (Xplora X6 Play/Anio 6) "
              "für eff. 6,46 €/Monat | 1 GB & Allnet-Flat [Nicotel]",
              "Xplora X6Play Nano SIM (2. Gen.)+Vodafone Smart Tech M – 1,61€/Monat",
              "Xplora XGO3 mit o2 Tarif, 69,95 € Zuzahlung"):
        assert ci_price.is_contract_deal(t), t


def test_plain_deals_not_contract():
    for t in ("Telekom Magenta Moments: Kinder-Smartwatch imoo Watch Phone Z7 für 149 € statt 199 €",
              "ECOVACS W3 OMNI Bestpreis",
              "Xplora X6 Play Kinder-Smartwatch für 99 €"):
        assert not ci_price.is_contract_deal(t), t


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

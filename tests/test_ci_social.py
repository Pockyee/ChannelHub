"""flows/ci_social.py 的纯函数测试（不连网、不连库）。

运行（worker 镜像里依赖齐全）:
  docker run --rm -v "$PWD:/repo" -w /repo channelhub-prefect-worker python tests/test_ci_social.py
也兼容 pytest。
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "flows"))

import ci_social  # noqa: E402


def test_yt_is_german_trusts_declared_language():
    assert ci_social.yt_is_german({"defaultAudioLanguage": "de", "title": "IMOO Z3 Unboxing"})
    assert ci_social.yt_is_german({"defaultAudioLanguage": "de-DE", "title": "x"})
    assert ci_social.yt_is_german({"defaultLanguage": "de-CH", "title": "x"})
    # 声明了别的语言：哪怕标题里有德语词也不要（imoo_UK、泰国配件店）
    assert not ci_social.yt_is_german({"defaultAudioLanguage": "en-GB",
                                       "title": "imoo Z3 für Kinder und Eltern"})
    assert not ci_social.yt_is_german({"defaultAudioLanguage": "th", "title": "เคส imoo Z7"})


def test_yt_is_german_falls_back_to_text():
    # 2026-09-28 首跑里的真实标题
    assert ci_social.yt_is_german({
        "title": "BEWERTUNG (2025): imoo Z3 Kinder-Smartwatch 4G. WESENTLICHE Einzelheiten",
        "description": "Die imoo Z3 ist eine Kinderuhr mit GPS und Videotelefonie."})
    assert ci_social.yt_is_german({
        "title": "Kids Smartwatch Vergleich: Ist die imoo Watch Z7 die Mini-Apple-Watch für Kids?"})
    assert not ci_social.yt_is_german({
        "title": "Recensione del imoo Z3 Orologio Smartwatch Bambini supporta SIM e GPS"})
    assert not ci_social.yt_is_german({"title": "IMOO Z3 Unboxing & First Look",
                                       "description": "Kinder smartwatch test"})
    assert not ci_social.yt_is_german({"title": "5 Rekomendasi Smartwatch Anak"})


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

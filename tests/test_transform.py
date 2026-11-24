import polars as pl
from conftest import make_payload

from nasa_power_pipeline.transform import build_features
from nasa_power_pipeline.validate import validate_weather


def test_features_are_computed_per_row():
    gold = build_features(validate_weather(make_payload(), "kumasi"))
    assert gold["is_rainy_day"].to_list() == [False, True, False]
    assert gold["dry_spell_days"].to_list() == [1, 0, 1]


def test_the_7_day_numbers_wait_for_7_days():
    gold = build_features(validate_weather(make_payload(days=8), "kumasi"))
    averages = gold["temp_7d_avg_c"].to_list()
    assert averages[:6] == [None] * 6
    assert averages[6] == 27.5  # mean of 26.0, 26.5, ... 29.0
    assert gold["rain_7d_mm"].to_list()[6] == 2.9  # 0.0 + 2.5 + 0.4 + 0 + 0 + 0 + 0
    assert gold.filter(pl.col("temp_7d_avg_c").is_not_null()).height == 2

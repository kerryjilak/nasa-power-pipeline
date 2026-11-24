from conftest import make_payload

from nasa_power_pipeline import load
from nasa_power_pipeline.validate import validate_weather


def test_loading_the_same_batch_twice_changes_nothing(tmp_path):
    database = str(tmp_path / "test.sqlite3")
    silver = validate_weather(make_payload(), "kumasi")

    load.save(database, silver)
    load.save(database, silver)

    assert load.count_rows(database) == {"weather_daily": 3, "weather_features": 3}
    assert str(load.latest_date(database, "kumasi")) == "2026-01-03"


def test_a_later_run_fills_in_a_minus_999_day(tmp_path):
    database = str(tmp_path / "test.sqlite3")

    load.save(database, validate_weather(make_payload(T2M=[26.0, 26.5, -999.0]), "kumasi"))
    assert temp_on(database, "2026-01-03") is None

    load.save(database, validate_weather(make_payload(), "kumasi"))
    assert temp_on(database, "2026-01-03") == 27.0
    assert load.count_rows(database) == {"weather_daily": 3, "weather_features": 3}


def temp_on(database: str, day: str) -> float | None:
    """Read one day's stored temperature."""
    with load.open_database(database) as (cursor, p):
        cursor.execute(f"SELECT temp_mean_c FROM weather_daily WHERE observed_on = {p}", (day,))
        return cursor.fetchone()[0]

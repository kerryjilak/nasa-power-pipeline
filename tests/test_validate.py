import pytest
from conftest import make_payload

from nasa_power_pipeline.validate import validate_weather


def test_a_good_batch_becomes_typed_silver_rows():
    silver = validate_weather(make_payload(), "kumasi")
    assert silver.height == 3
    assert silver.columns == [
        "location",
        "observed_on",
        "temp_mean_c",
        "humidity_pct",
        "precipitation_mm",
        "wind_speed_ms",
    ]
    assert str(silver["observed_on"].dtype) == "Date"


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"T2M": [26.0, 99.0, 27.0]}, "temperature is outside"),
        ({"RH2M": [80.0, 140.0, 81.0]}, "humidity is outside"),
        ({"PRECTOTCORR": [0.0, -1.0, 0.0]}, "precipitation is negative"),
        ({"WS10M": [1.5, -2.0, 2.5]}, "wind speed is negative"),
        ({"T2M": [26.0, None, 27.0]}, "missing or has the wrong type"),
        ({"WS10M": [1.5, 2.0]}, "cover different dates"),
    ],
)
def test_one_bad_row_rejects_the_whole_batch(changes, message):
    with pytest.raises(ValueError, match=message):
        validate_weather(make_payload(**changes), "kumasi")


def test_minus_999_becomes_null_and_is_allowed():
    silver = validate_weather(make_payload(T2M=[26.0, 26.5, -999.0]), "kumasi")
    assert silver.height == 3
    assert silver["temp_mean_c"].null_count() == 1


def test_a_missing_field_is_named():
    payload = make_payload()
    del payload["properties"]["parameter"]["PRECTOTCORR"]
    with pytest.raises(ValueError, match="missing fields: PRECTOTCORR"):
        validate_weather(payload, "kumasi")

import json
from datetime import date

import httpx
import pytest

from nasa_power_pipeline import extract


def test_bronze_keeps_the_payload_and_where_it_came_from(tmp_path):
    payload = {"properties": {"parameter": {"T2M": {"20260101": 26.5}}}}

    path = extract.save_bronze(payload, "kumasi", str(tmp_path))

    record = json.loads(path.read_text())
    assert record["payload"] == payload
    assert record["_location"] == "kumasi"
    assert "_ingested_at" in record
    assert "location=kumasi" in str(path)


def fake_answer(status_code):
    """Replace httpx.get with a function that returns a fixed status, without the internet."""

    def fake_get(url, params, timeout):
        return httpx.Response(status_code, json={}, request=httpx.Request("GET", url))

    return fake_get


def test_a_server_error_is_temporary(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(503))
    with pytest.raises(extract.TemporarySourceError):
        extract.fetch_weather(6.69, -1.62, date(2026, 1, 1), date(2026, 1, 3))


def test_a_bad_request_is_permanent(monkeypatch):
    monkeypatch.setattr(httpx, "get", fake_answer(422))
    with pytest.raises(httpx.HTTPStatusError):
        extract.fetch_weather(6.69, -1.62, date(2026, 1, 1), date(2026, 1, 3))

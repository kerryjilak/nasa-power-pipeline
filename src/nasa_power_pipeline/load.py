"""Load: save Silver and Gold so that loading the same batch twice changes nothing."""

import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date
from pathlib import Path

import polars as pl

from nasa_power_pipeline.transform import build_features

sqlite3.register_adapter(date, date.isoformat)  # Store dates as YYYY-MM-DD text in SQLite.

# Value columns allow NULL: NASA's -999 ("no value yet") becomes NULL in Silver,
# and a later run overwrites it once NASA has the number.
TABLES = """
CREATE TABLE IF NOT EXISTS weather_daily (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_mean_c DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (location, observed_on)
);
CREATE TABLE IF NOT EXISTS weather_features (
    location TEXT NOT NULL,
    observed_on DATE NOT NULL,
    temp_7d_avg_c DOUBLE PRECISION,
    rain_7d_mm DOUBLE PRECISION,
    is_rainy_day BOOLEAN,
    dry_spell_days INTEGER,
    PRIMARY KEY (location, observed_on)
);
"""
# {p} becomes the driver's placeholder: ? for SQLite (and %s for PostgreSQL in Lab 9).
UPSERT_SILVER = """
INSERT INTO weather_daily
    (location, observed_on, temp_mean_c, humidity_pct, precipitation_mm, wind_speed_ms)
VALUES ({p}, {p}, {p}, {p}, {p}, {p})
ON CONFLICT (location, observed_on) DO UPDATE SET
    temp_mean_c = excluded.temp_mean_c,
    humidity_pct = excluded.humidity_pct,
    precipitation_mm = excluded.precipitation_mm,
    wind_speed_ms = excluded.wind_speed_ms,
    loaded_at = CURRENT_TIMESTAMP
"""
UPSERT_GOLD = """
INSERT INTO weather_features
    (location, observed_on, temp_7d_avg_c, rain_7d_mm, is_rainy_day, dry_spell_days)
VALUES ({p}, {p}, {p}, {p}, {p}, {p})
ON CONFLICT (location, observed_on) DO UPDATE SET
    temp_7d_avg_c = excluded.temp_7d_avg_c,
    rain_7d_mm = excluded.rain_7d_mm,
    is_rainy_day = excluded.is_rainy_day,
    dry_spell_days = excluded.dry_spell_days
"""
# Column types for reading Silver back, so a column that is all NULL still counts as a number.
SILVER_SCHEMA = {
    "location": pl.String,
    "observed_on": pl.String,
    "temp_mean_c": pl.Float64,
    "humidity_pct": pl.Float64,
    "precipitation_mm": pl.Float64,
    "wind_speed_ms": pl.Float64,
}
SILVER_COLUMNS = list(SILVER_SCHEMA)


@contextmanager
def open_database(database: str) -> Iterator[tuple[sqlite3.Cursor, str]]:
    """Open the database, create the tables, and commit only if everything succeeds."""
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database)) as connection:
        connection.executescript(TABLES)
        with connection:  # Commits on success, rolls back on any error.
            yield connection.cursor(), "?"


def save(database: str, silver: pl.DataFrame) -> int:
    """Upsert Silver, rebuild Gold from all stored Silver, and commit both together.

    Returns:
        The number of Gold rows written.
    """
    locations = silver["location"].unique().to_list()
    with open_database(database) as (cursor, p):
        cursor.executemany(UPSERT_SILVER.format(p=p), silver.select(SILVER_COLUMNS).rows())
        query = f"SELECT {', '.join(SILVER_COLUMNS)} FROM weather_daily WHERE location = {p}"
        history = []
        for location in locations:
            cursor.execute(query, (location,))
            history += cursor.fetchall()
        stored = pl.DataFrame(history, schema=SILVER_SCHEMA, orient="row").with_columns(
            pl.col("observed_on").str.to_date()
        )
        gold = build_features(stored)
        cursor.executemany(UPSERT_GOLD.format(p=p), gold.rows())
    return gold.height


def latest_date(database: str, location: str) -> date | None:
    """Return the newest stored date for a location: the pipeline's watermark."""
    with open_database(database) as (cursor, p):
        cursor.execute(
            f"SELECT MAX(observed_on) FROM weather_daily WHERE location = {p}", (location,)
        )
        (latest,) = cursor.fetchone()
    return date.fromisoformat(str(latest)) if latest else None


def count_rows(database: str) -> dict[str, int]:
    """Count the rows in each table: a quick check that reruns add nothing."""
    with open_database(database) as (cursor, _):
        counts = {}
        for table in ("weather_daily", "weather_features"):
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cursor.fetchone()[0]
    return counts

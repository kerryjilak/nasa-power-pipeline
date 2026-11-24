"""Transform: compute Gold features from the full Silver history."""

import polars as pl


def build_features(silver: pl.DataFrame) -> pl.DataFrame:
    """Add the values a farmer, analyst or model will use.

    Pass the whole Silver history for a location, not one batch, so that a 7-day
    average at the start of a new batch can still see the days before it.
    """
    rainy = pl.col("precipitation_mm") >= 1.0
    return (
        silver.sort("location", "observed_on")
        .with_columns(
            pl.col("temp_mean_c")
            .rolling_mean(7)
            .over("location")
            .round(2)
            .alias("temp_7d_avg_c"),
            pl.col("precipitation_mm")
            .rolling_sum(7)
            .over("location")
            .round(1)
            .alias("rain_7d_mm"),
            rainy.alias("is_rainy_day"),
            rainy.cum_sum().over("location").alias("rainy_days_so_far"),
        )
        .with_columns(
            # Count dry days since the last rainy day. Each rainy day starts a new count.
            (~pl.col("is_rainy_day"))
            .cast(pl.Int32)
            .cum_sum()
            .over("location", "rainy_days_so_far")
            .alias("dry_spell_days"),
        )
        .select(
            "location",
            "observed_on",
            "temp_7d_avg_c",
            "rain_7d_mm",
            "is_rainy_day",
            "dry_spell_days",
        )
    )

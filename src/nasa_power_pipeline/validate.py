"""Validate: turn the raw response into a typed Silver table, or reject the whole batch."""

import polars as pl

FIELDS = {
    "T2M": "temp_mean_c",
    "RH2M": "humidity_pct",
    "PRECTOTCORR": "precipitation_mm",
    "WS10M": "wind_speed_ms",
}
MISSING = -999.0  # NASA POWER's marker for "no value for this day yet".
KEY = ["location", "observed_on"]


def validate_weather(payload: dict, location: str) -> pl.DataFrame:
    """Check the data contract and return clean Silver rows.

    A -999 becomes null, because NASA often has no value yet for the newest days.
    That is allowed. Any other missing value or wrong type rejects the batch.

    Raises:
        ValueError: A field is missing or any row breaks a rule. One bad row rejects the batch.
    """
    properties = payload.get("properties")
    parameter = properties.get("parameter") if isinstance(properties, dict) else None
    if not isinstance(parameter, dict):
        parameter = {}  # Treat a missing or malformed section as "every field is missing".
    missing = sorted(FIELDS.keys() - parameter.keys())
    if missing:
        raise ValueError(f"{location}: missing fields: {', '.join(missing)}.")
    if not all(isinstance(parameter[name], dict) for name in FIELDS):
        raise ValueError(f"{location}: a field is not a date-to-value mapping.")
    dates = sorted(parameter["T2M"])
    if any(sorted(parameter[name]) != dates for name in FIELDS):
        raise ValueError(f"{location}: the fields cover different dates.")

    # NASA sends {field: {"YYYYMMDD": value}}. Turn it into one row per day.
    raw = pl.DataFrame(
        {"date": dates, **{name: [parameter[name][d] for d in dates] for name in FIELDS}},
        strict=False,
    )
    typed = raw.select(
        pl.lit(location).alias("location"),
        pl.col("date").cast(pl.String).str.to_date("%Y%m%d", strict=False).alias("observed_on"),
        *[
            pl.col(source).cast(pl.Float64, strict=False).alias(target)
            for source, target in FIELDS.items()
        ],
    )
    # Checked before -999 becomes null, so a null here is a real problem.
    _reject_bad_rows(
        typed,
        {"a value is missing or has the wrong type": pl.any_horizontal(pl.all().is_null())},
        location,
    )

    silver = typed.with_columns(
        pl.when(pl.col(target) == MISSING).then(None).otherwise(pl.col(target)).alias(target)
        for target in FIELDS.values()
    )
    _reject_bad_rows(
        silver,
        {
            "temperature is outside -90 to 60 °C": ~pl.col("temp_mean_c").is_between(-90, 60),
            "humidity is outside 0 to 100 %": ~pl.col("humidity_pct").is_between(0, 100),
            "precipitation is negative": pl.col("precipitation_mm") < 0,
            "wind speed is negative": pl.col("wind_speed_ms") < 0,
            "the same location and date appear twice": pl.struct(KEY).is_duplicated(),
        },
        location,
    )
    return silver.sort(KEY)


def _reject_bad_rows(frame: pl.DataFrame, rules: dict, location: str) -> None:
    """Raise ValueError for the first rule that any row breaks."""
    for problem, broken in rules.items():
        bad_rows = frame.filter(broken)
        if bad_rows.height:
            examples = bad_rows["observed_on"].head(3).to_list()
            raise ValueError(f"{location}: {problem} in {bad_rows.height} row(s), e.g. {examples}.")

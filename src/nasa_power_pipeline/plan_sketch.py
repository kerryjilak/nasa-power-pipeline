"""A sketch of the real pipeline: each step only prints what it will do."""

from prefect import flow, task


@task(retries=2, retry_delay_seconds=3)
def fetch_daily_weather() -> str:
    """Will call the NASA POWER daily API for Kumasi."""
    print("Will fetch T2M, RH2M, PRECTOTCORR, WS10M for the last 5 days")
    return "raw response"


@task
def save_bronze(raw: str) -> str:
    """Will keep the response exactly as received."""
    print("Will save the raw JSON, the request URL and the fetch time (Bronze)")
    return raw


@task
def validate_and_clean(raw: str) -> str:
    """Will check the contract rules and build typed rows."""
    print("Will check the rules, turn -999 into null, make one row per day (Silver)")
    return "silver rows"


@task
def build_gold_metrics(rows: str) -> str:
    """Will turn daily rows into the numbers a farmer would use."""
    print("Will calculate 7-day average temperature, 7-day rainfall, dry spell (Gold)")
    return "gold rows"


@flow(log_prints=True)
def nasa_power_plan() -> str:
    """Run fetch, Bronze, Silver and Gold in order."""
    raw = fetch_daily_weather()
    saved = save_bronze(raw)
    silver = validate_and_clean(saved)
    return build_gold_metrics(silver)


if __name__ == "__main__":
    nasa_power_plan()
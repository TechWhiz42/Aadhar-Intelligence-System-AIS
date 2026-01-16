import pandas as pd
from pathlib import Path


# Resolve project paths safely

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

MASTER_DAILY_PATH = PROCESSED_DIR / "master_district_daily.csv"

if not MASTER_DAILY_PATH.exists():
    raise RuntimeError(
        "master_district_daily.csv not found. "
        "Run daily merge step first."
    )


# Load daily master data

daily_df = pd.read_csv(MASTER_DAILY_PATH)

# Ensure date is datetime
daily_df["date"] = pd.to_datetime(
    daily_df["date"],
    errors="coerce"
)


# Monthly aggregation

daily_df["year_month"] = daily_df["date"].dt.to_period("M")

monthly_agg = (
    daily_df
    .groupby(["year_month", "state", "district"], as_index=False)
    .agg({
        "demographic_count": "sum",
        "enrolment_count": "sum",
        "biometric_count": "sum",
        "demo_age_5_17": "sum",
        "demo_age_17_": "sum"
    })
)

# Convert Period to timestamp (month start)
monthly_agg["year_month"] = monthly_agg["year_month"].dt.to_timestamp()


# Save monthly aggregated data

monthly_agg.to_csv(
    PROCESSED_DIR / "master_district_monthly.csv",
    index=False
)

print("✅ Monthly aggregation completed successfully")

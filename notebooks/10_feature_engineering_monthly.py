import pandas as pd
import numpy as np
from pathlib import Path


# Resolve project paths safely

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

MONTHLY_PATH = PROCESSED_DIR / "master_district_monthly.csv"

if not MONTHLY_PATH.exists():
    raise RuntimeError(
        "master_district_monthly.csv not found. "
        "Run monthly aggregation first."
    )


# Load monthly master dataset

df = pd.read_csv(MONTHLY_PATH)


# Schema validation

required_cols = {
    "demographic_count",
    "enrolment_count",
    "biometric_count",
    "demo_age_5_17",
    "demo_age_17_",
    "year_month"
}

missing = required_cols - set(df.columns)
if missing:
    raise RuntimeError(f"Missing required columns: {missing}")


# Feature engineering (monthly)

features = df.copy()

features["enrolment_pressure"] = (
    features["enrolment_count"] /
    features["demographic_count"].replace(0, np.nan)
).fillna(0)

features["biometric_load_ratio"] = (
    features["biometric_count"] /
    features["enrolment_count"].replace(0, np.nan)
).fillna(0)

features["youth_population_ratio"] = (
    features["demo_age_5_17"] /
    features["demographic_count"].replace(0, np.nan)
).fillna(0)

features["adult_population_ratio"] = (
    features["demo_age_17_"] /
    features["demographic_count"].replace(0, np.nan)
).fillna(0)


# Temporal features

features["year_month"] = pd.to_datetime(
    features["year_month"],
    errors="coerce"
)

features["year"] = features["year_month"].dt.year
features["month"] = features["year_month"].dt.month


# Monthly signal

features["high_enrolment_pressure"] = (
    features["enrolment_pressure"] >
    features["enrolment_pressure"].quantile(0.75)
).astype(int)

# Save monthly features

features.to_csv(
    PROCESSED_DIR / "master_features_district_monthly.csv",
    index=False
)

print("✅ Monthly feature engineering completed successfully")

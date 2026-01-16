import pandas as pd
import numpy as np
from pathlib import Path


# Resolve project paths safely

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

MASTER_PATH = PROCESSED_DIR / "master_district_daily.csv"

if not MASTER_PATH.exists():
    raise RuntimeError(
        "master_district_daily.csv not found. Run merge step first."
    )


# Load master daily dataset

df = pd.read_csv(MASTER_PATH)


# Schema validation (this will FAIL if wrong file is used)

required_cols = {
    "demographic_count",
    "enrolment_count",
    "biometric_count",
    "demo_age_5_17",
    "demo_age_17_",
    "date"
}

missing = required_cols - set(df.columns)
if missing:
    raise RuntimeError(f"Missing required columns: {missing}")


# Feature engineering (USES demographic_count ONLY)

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

features["date"] = pd.to_datetime(features["date"], errors="coerce")
features["day_of_week"] = features["date"].dt.dayofweek
features["month"] = features["date"].dt.month


# Binary signal

features["high_enrolment_ratio"] = (
    features["enrolment_pressure"] >
    features["enrolment_pressure"].quantile(0.75)
).astype(int)


# Save features

features.to_csv(
    PROCESSED_DIR / "master_features_district_daily.csv",
    index=False
)

print("✅ Daily feature engineering completed successfully")

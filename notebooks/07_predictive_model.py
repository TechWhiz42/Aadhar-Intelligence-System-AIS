import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.ensemble import IsolationForest
import joblib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
MODEL_DIR = PROJECT_ROOT / "models"

MODEL_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_PATH = PROCESSED_DIR / "master_features_district_daily.csv"

if not FEATURE_PATH.exists():
    raise RuntimeError(
        "master_features_district_daily.csv not found. "
        "Run feature engineering first."
    )

df = pd.read_csv(FEATURE_PATH)

feature_cols = [
    "enrolment_pressure",
    "biometric_load_ratio",
    "youth_population_ratio",
    "adult_population_ratio"
]

missing = set(feature_cols) - set(df.columns)
if missing:
    raise RuntimeError(f"Missing required feature columns: {missing}")

X = df[feature_cols]

# Scale features (robust to outliers)
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Train Isolation Forest

model = IsolationForest(
    n_estimators=200,
    contamination=0.05,
    random_state=42
)

df["anomaly_score"] = model.fit_predict(X_scaled)
df["is_anomaly"] = (df["anomaly_score"] == -1).astype(int)

# Save outputs

df.to_csv(
    PROCESSED_DIR / "daily_anomaly_results.csv",
    index=False
)

joblib.dump(
    model,
    MODEL_DIR / "isolation_forest_daily.joblib"
)

joblib.dump(
    scaler,
    MODEL_DIR / "scaler_daily.joblib"
)

print("✅ Anomaly detection model trained and results saved")

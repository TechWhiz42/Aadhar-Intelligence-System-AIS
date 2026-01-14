
import pandas as pd
from pathlib import Path
import joblib

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

DATA_PATH = Path("../data/processed")

df = pd.read_csv("../data/processed/master_features_district_daily.csv")

print(df.shape)
print(df.columns.tolist())


df = df.drop(columns=["level_0", "index"], errors="ignore")


id_cols = ['date', 'state', 'district']

ids = df[id_cols]
X = df.drop(columns=id_cols)

X = X.select_dtypes(include="number")
print("Features matrix shape: ", X.shape)

X = X.fillna(X.median())

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

iso = IsolationForest(
    n_estimators=200,
    contamination=0.05,
    random_state=42,
    n_jobs=-1
)

iso.fit(X_scaled)

df["anomaly_score"] = iso.decision_function(X_scaled)


df["anomaly_flag"] = (iso.predict(X_scaled) == -1).astype(int)

df["anomaly_flag"].value_counts()

print(df[df["anomaly_flag"]==1]\
    .sort_values("anomaly_score")\
    .head(10)[
        ['date', 'state', 'district', 'anomaly_score']
])

MODEL_DIR = Path("../models")
MODEL_DIR.mkdir(exist_ok=True)

joblib.dump(iso, MODEL_DIR / 'isolation_forest.pkl')
joblib.dump(scaler, MODEL_DIR / 'scaler.pkl')
joblib.dump(X.columns.tolist(), MODEL_DIR / 'feature_list.pkl')

OUTPUT_DIR = Path("../outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

output_df = df[
    ['date', 'state', 'district', 'anomaly_score', 'anomaly_flag']
]

output_df.head()

output_df.to_csv(
    OUTPUT_DIR / 'anomaly_scores_district_daily.csv',
    index=False
)

output_df["anomaly_flag"].value_counts()

top_districts = (
    output_df[output_df["anomaly_flag"] == 1]
    .groupby(["state", "district"])
    .size()
    .reset_index(name="anomaly_days")
    .sort_values("anomaly_days", ascending=False)
)

top_districts.head(10)


top_districts.to_csv(
    OUTPUT_DIR / 'top_anomalous_districts.csv',
    index=False
)

#dow = day of week

anomalies_by_dow = (
    df[df["anomaly_flag"] == 1]
    .groupby("day_of_week")
    .size()
    .reset_index(name="count")
)
print(anomalies_by_dow)


anomalies_by_month = (
    df[df["anomaly_flag"] == 1]
    .groupby("month")
    .size()
    .reset_index(name="count")
)
print(anomalies_by_month)


feature_summary = pd.DataFrame({
    "normal_mean": df[df["anomaly_flag"]==0][X.columns].mean(),
    "anomalous_mean": df[df["anomaly_flag"]==1][X.columns].mean()
})

feature_summary["difference"] = (
    feature_summary["anomalous_mean"] - feature_summary["normal_mean"]
)

feature_summary.sort_values("difference", ascending=False)

feature_summary.to_csv(
    OUTPUT_DIR / "feature_impact_summary.csv"
)


# Districts flagged as anomalous show significantly higher enrolment pressure and biometric load ratios compared to normal days, indicating operational stress or irregular activity


import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(layout="wide", page_title="District Anomaly Dashboard")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "processed"

df = pd.read_csv(DATA_DIR / "daily_anomaly_results.csv")
df["date"] = pd.to_datetime(df["date"])

st.markdown(
    """
    <style>
    section[data-testid="stSidebar"] div[data-baseweb="select"] * {
        cursor: pointer !important;
    }
    </style>
    """,
    unsafe_allow_html=True
)

states = sorted(
    s for s in df["state"].astype(str).unique()
    if any(c.isalpha() for c in s)
)

st.sidebar.title("Filters")

state = st.sidebar.selectbox("State", states)

districts = (
    df[df["state"] == state]["district"]
    .astype(str)
    .unique()
)

districts = sorted(
    d for d in districts if any(c.isalpha() for c in d)
)

district = st.sidebar.selectbox("District", districts)

date_range = st.sidebar.date_input(
    "Date Range",
    [df["date"].min(), df["date"].max()]
)

filtered = df[
    (df["state"] == state) &
    (df["district"] == district) &
    (df["date"].between(
        pd.to_datetime(date_range[0]),
        pd.to_datetime(date_range[1])
    ))
]

st.title(f"{district}, {state}")

c1, c2, c3 = st.columns(3)

c1.metric(
    "Anomaly % Days",
    f"{filtered['is_anomaly'].mean() * 100:.1f}%"
)

c2.metric(
    "Avg Enrolment Pressure",
    round(filtered["enrolment_pressure"].mean(), 2)
)

c3.metric(
    "Max Biometric Load",
    round(filtered["biometric_load_ratio"].max(), 2)
)

fig = px.line(
    filtered,
    x="date",
    y="enrolment_pressure",
    markers=True
)

anomalies = filtered[filtered["is_anomaly"] == 1]

fig.add_scatter(
    x=anomalies["date"],
    y=anomalies["enrolment_pressure"],
    mode="markers",
    marker=dict(size=10, color="red"),
    name="Anomaly"
)

st.plotly_chart(fig, use_container_width=True)

feature_cols = [
    "enrolment_pressure",
    "biometric_load_ratio",
    "youth_population_ratio",
    "adult_population_ratio"
]

if not filtered.empty:
    latest = filtered.sort_values("date").iloc[-1]

    feature_df = pd.DataFrame({
        "Feature": feature_cols,
        "Value": [latest[col] for col in feature_cols]
    })

    st.subheader("Latest Feature Snapshot")
    st.bar_chart(feature_df.set_index("Feature"))

top = (
    df[df["date"].between(
        pd.to_datetime(date_range[0]),
        pd.to_datetime(date_range[1])
    )]
    .groupby(["state", "district"])
    .agg(
        anomaly_days=("is_anomaly", "sum"),
        avg_pressure=("enrolment_pressure", "mean")
    )
    .reset_index()
)

top = top[
    top["district"].astype(str).apply(lambda x: any(c.isalpha() for c in x))
].sort_values("anomaly_days", ascending=False).head(10)

st.subheader("Top Anomalous Districts")
st.dataframe(top)

Here’s a **clean, professional README** you can copy-paste directly.
It’s concise, technical, and interview-ready.

---

````md
# District-Level Anomaly Detection Dashboard

An end-to-end data pipeline and interactive dashboard for detecting and monitoring
district-level anomalies using government enrolment, biometric, and demographic data.

This project demonstrates real-world data engineering, feature engineering,
unsupervised machine learning, and visualization practices.

---

## Project Overview

The system ingests public government datasets via APIs, cleans and normalizes the data,
aggregates it at daily and monthly district levels, engineers meaningful features,
detects anomalies using an Isolation Forest model, and visualizes the results through
an interactive Streamlit dashboard.

## Pipeline Architecture

API Ingestion  
→ Raw Data Storage (batch-wise, resumable)  
→ Data Cleaning & Validation  
→ Daily District Aggregation  
→ Feature Engineering  
→ Anomaly Detection (Isolation Forest)  
→ Interactive Dashboard


## Key Features

- Resumable API ingestion with retry logic
- Robust data cleaning and schema validation
- State and district normalization framework
- Daily and monthly aggregations
- Feature engineering for pressure and load ratios
- Unsupervised anomaly detection using Isolation Forest
- Interactive district-level dashboard using Streamlit
- UI-level sanitation without mutating analytical data

## Technologies Used

- Python
- Pandas, NumPy
- Scikit-learn
- Streamlit
- Plotly
- Requests
- Git & GitHub

## Dashboard Capabilities

- State and district selection
- Date-range filtering
- Anomaly percentage and KPI metrics
- Time-series visualization with anomaly markers
- Feature-level explainability
- Ranking of top anomalous districts


## Running the Dashboard

From the project root:

```bash
streamlit run dashboard/district_anomaly_dashboard.py
````

The dashboard will be available at:

```
http://localhost:8501
```

## Repository Structure

```
├── src/
│   ├── api_ingestion/
│   └── utils/
├── notebooks/
├── dashboard/
│   └── district_anomaly_dashboard.py
├── data/
│   ├── raw/          # ignored in git
│   ├── processed/    # ignored in git
│   └── reference/
├── models/           # ignored in git
├── README.md
└── requirements.txt
```

## Notes

* API keys are stored securely using environment variables and are not committed.
* Raw and processed data directories are excluded from version control.
* The dashboard reflects data truthfully and does not mask upstream data issues.

## Future Enhancements

* Monthly vs daily dashboard toggle
* Geographic heatmaps
* Automated anomaly alerts
* Data quality monitoring dashboard
* Explainability using SHAP values

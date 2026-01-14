
import pandas as pd
import numpy as np

master_df = "../data/processed/master_district_daily.csv"

master_df = pd.read_csv(master_df)


master_features = master_df.copy()

master_features['enrolment_pressure'] = (
    master_features['enrolment_count'] /
    master_features['total_demographic'].replace(0, np.nan)
).fillna(0).infer_objects(copy=False)


master_features['biometric_load_ratio'] = (
    master_features['biometric_count'] /
    master_features['enrolment_count'].replace(0, np.nan)
).fillna(0)

master_features['youth_population_ratio'] = (
    master_features['demo_age_5_17'] /
    master_features['total_demographic'].replace(0, np.nan)
).fillna(0)

master_features['adult_enrolment_ratio'] = (
    master_features['demo_age_17_']/
    master_features['total_demographic'].replace(0, np.nan)
).fillna(0)


master_features['date'] = pd.to_datetime(
    master_features['date'],
    errors='coerce'
)

master_features['day_of_week'] = master_features['date'].dt.dayofweek

master_features['month'] = master_features['date'].dt.month


master_features['high_enrolment_ratio'] = (
    master_features['enrolment_pressure'] >
    master_features['enrolment_pressure'].quantile(0.75)
).astype(int)

master_features.describe()


master_features.to_csv(
    "../data/processed/master_features_district_daily.csv",
    index=False
)



import pandas as pd

demographic_agg = pd.read_csv("../data/processed/demographic_agg.csv")
enrolment_agg = pd.read_csv("../data/processed/enrolment_agg.csv")
biometric_agg = pd.read_csv("../data/processed/biometric_agg.csv")

master_df = demographic_agg.copy()

master_df = master_df.merge(
    enrolment_agg,
    on=['date', 'state','district'],
    how='left'
)

biometric_agg = biometric_agg.reset_index()

master_df = master_df.merge(
    biometric_agg,
    on=['date', 'state','district'],
    how='left'
)


numeric_cols = master_df.select_dtypes(include='number').columns
master_df[numeric_cols] = master_df[numeric_cols].fillna(0)

master_df.duplicated(
    subset=['date', 'state', 'district']
).sum()

master_df.to_csv(
    "../data/processed/master_district_daily.csv",
    index=False
)



import  pandas as pd
import glob

path = "../data/raw/enrolment/*.csv"
files = glob.glob(path)

print(len(files))

df_list = [pd.read_csv(f) for f in files]
enrolment_df = pd.concat(df_list, ignore_index=True)

enrolment_clean = enrolment_df.copy()

text_cols = ['state', 'district']

for col in text_cols:
    enrolment_clean[col]= (
        enrolment_clean[col]
        .astype(str)
        .str.strip()
        .str.title()
    )

enrolment_clean['date'] = pd.to_datetime(
    enrolment_clean['date'],
    errors='coerce'
)

enrolment_clean.isna().sum()

enrolment_clean = enrolment_clean.dropna(
    subset=['state','district']
)

age_cols = ['age_0_5', 'age_5_17', 'age_18_greater']

enrolment_clean[age_cols] = enrolment_clean[age_cols].fillna(0)

for col in age_cols:
    enrolment_clean = enrolment_clean[
        enrolment_clean[col]>=0
    ]

enrolment_clean['enrolment_count'] = (
    enrolment_clean[age_cols].sum(axis=1)
)

before = enrolment_clean.shape[0]
enrolment_clean = enrolment_clean.drop_duplicates()
after = enrolment_clean.shape[0]

print("Duplicates removed: ", before- after)

enrolment_clean.info()
enrolment_clean.describe()

enrolment_clean.to_csv(
    "../data/processed/enrolment_clean.csv",
    index=False
)

enrolment_agg = (
    enrolment_clean
    .groupby(['date', 'state', 'district'], as_index=False)
    .agg({
        'age_0_5': 'sum',
        'age_5_17': 'sum',
        'age_18_greater': 'sum',
        'enrolment_count': 'sum'
    })
)

enrolment_agg.head(), enrolment_agg.shape

enrolment_agg.to_csv(
    "../data/processed/enrolment_agg.csv",
    index=False
)

df = enrolment_agg

df.duplicated(['date', 'state', 'district']).sum()


import pandas as pd
import glob

path = "../data/raw/biometric/*.csv"
files = glob.glob(path)

print(len(files))

df_list = [pd.read_csv(f) for f in files]

biometric_df = pd.concat(df_list, ignore_index=True)
print("Total rows: ", biometric_df.shape[0])
print("Total columns: ", biometric_df.shape[1])


biometric_df.head()


biometric_clean = biometric_df.copy()

text_cols = ['state', 'district']

for col in text_cols:
    biometric_clean[col] = (
        biometric_clean[col]
        .astype(str)
        .str.strip()
        .str.title()
    )

biometric_clean['date'] = pd.to_datetime(
    biometric_clean['date'],
    errors='coerce'
)

biometric_clean.isna().sum()

biometric_clean = biometric_clean.dropna(
    subset = ['state', 'district']
)

bio_cols = ['bio_age_5_17', 'bio_age_17_']
biometric_clean[bio_cols] = biometric_clean[bio_cols].fillna(0)

for col in bio_cols:
    biometric_clean = biometric_clean[
        biometric_clean[col]>=0
    ]

biometric_clean['biometric_count'] = (
    biometric_clean['bio_age_5_17']+
    biometric_clean['bio_age_17_']
)

before = biometric_clean.shape[0]
biometric_clean = biometric_clean.drop_duplicates()
after = biometric_clean.shape[0]

print("Duplicates removed: ", before-after)

biometric_clean.info()
biometric_clean.describe()


biometric_clean.to_csv(
    "../data/processed/biometric_clean.csv",
    index=False
)

biometric_agg = (
    biometric_clean
    .groupby(['date', 'state', 'district'])
    .agg({
        'bio_age_5_17': 'sum',
        'bio_age_17_': 'sum',
        'biometric_count': 'sum'
    })
)

biometric_agg.head(), biometric_agg.shape

biometric_agg.to_csv(
    "../data/processed/biometric_agg.csv",
    index=False
)



biometric_agg = (
    biometric_clean
    .groupby(['date', 'state', 'district'], as_index=False)
    .agg({
        'bio_age_5_17': 'sum',
        'bio_age_17_': 'sum',
        'biometric_count': 'sum'
    })
)


df = biometric_agg

df.duplicated(['date','state', 'district']).sum()


biometric_agg.to_csv(
    "../data/processed/biometric_agg.csv",
    index=False
)
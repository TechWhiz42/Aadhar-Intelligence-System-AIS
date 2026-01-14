
import pandas as pd
import glob

# Path to demographic files
path = "../data/raw/demographic/*.csv"
files = glob.glob(path)

print(files)




df_list = [pd.read_csv(file) for file in files]



demographic_df = pd.concat(df_list, ignore_index = True)
print("Total rows" , demographic_df.shape[0])
print("Total columns: ", demographic_df.shape[1])

print(demographic_df.head())


print(demographic_df.info())

demographic_df['date'] = pd.to_datetime(demographic_df['date'], errors='coerce')
print(demographic_df.head())

demographic_df['demo_age_5_17'] = pd.to_numeric(demographic_df['demo_age_5_17'], errors='coerce')
print(demographic_df.head())

demographic_df['demo_age_17_'] = pd.to_numeric(
    demographic_df['demo_age_17_'], errors='coerce'
)

print(demographic_df.head())

print(demographic_df.isna().sum())

print(demographic_df.isna())

duplicate_count = demographic_df.duplicated().sum()
print("Duplicate rows:", duplicate_count)


negative_5_17 = (demographic_df['demo_age_5_17']<0).sum()
negative_17_plus = (demographic_df['demo_age_17_']<0).sum()
print("Negative values (5-17): ", negative_5_17)
print("Negative values (17-plus): ", negative_17_plus)

demographic_df['total_demo'] = (
    demographic_df['demo_age_5_17'] + demographic_df['demo_age_17_']
)

demographic_df['total_demo'].describe()

# "I validated demographic data by checking schema consistency, missing values, duplicates,and basic numeric sanity. No transformations were applied at this stage to preserve the raw data integrity"

print(demographic_df.shape)

demographic_clean = demographic_df.copy()

text_cols = ['state', 'district']

for col in text_cols:
    demographic_clean[col] = (
        demographic_clean[col]
        .str.strip()
        .str.title()
    )

demographic_clean.isna().sum()

demographic_clean = demographic_clean.dropna(
    subset=['state', 'district']
)

count_cols = ['demo_age_5_17', 'demo_age_17_']
demographic_clean[count_cols] = demographic_clean[count_cols].fillna(0)

for col in count_cols:
    demographic_clean = demographic_clean[
        demographic_clean[col] >= 0
    ]

demographic_clean['total_demographic'] = (
    demographic_clean['demo_age_5_17'] +
    demographic_clean['demo_age_17_']
)


before = demographic_clean.shape[0]
demographic_clean = demographic_clean.drop_duplicates()
after = demographic_clean.shape[0]

print(f"Duplicates removed: {before-after}")

demographic_clean.info()
demographic_clean.describe()


demographic_clean.to_csv(
    "../data/processed/demographic_clean.csv",
    index=False
)

# "Demographic data was cleaned by standardising geographic names, handling missing values conservatively, removing invalid records, and creating derived population metrics. Raw data was preserved."

demographic_agg = (
    demographic_clean
    .groupby(['date', 'state', 'district'], as_index=False)
    .agg({
        'demo_age_5_17': 'sum',
        'demo_age_17_': 'sum',
        'total_demographic': 'sum'
    })
)
demographic_agg.head(), demographic_agg.shape

demographic_agg.to_csv(
    "../data/processed/demographic_agg.csv",
    index=False
)

df = demographic_agg
df.duplicated(subset=['date', 'state', 'district']).sum()


master_df = demographic_agg.copy()
enrolment_agg = pd.read_csv("../data/processed/enrolment_agg.csv")
enrolment_agg['date'] = pd.to_datetime(enrolment_agg['date'], errors='coerce')

master_df = master_df.merge(
    enrolment_agg,
    on=['date', 'state', 'district'],
    how='left'
)
import pandas as pd
import glob
from pathlib import Path


# Resolve project paths safely (independent of working directory)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "enrolment"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Ensure output directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# Load raw enrolment data

files = glob.glob(str(RAW_DIR / "*.csv"))

if not files:
    raise RuntimeError(f"No enrolment raw files found in {RAW_DIR}")

df_list = [pd.read_csv(f) for f in files]
enrolment_df = pd.concat(df_list, ignore_index=True)


# Cleaning

enrolment_clean = enrolment_df.copy()

# Standardise text fields
text_cols = ["state", "district"]
for col in text_cols:
    enrolment_clean[col] = (
        enrolment_clean[col]
        .astype(str)
        .str.strip()
        .str.title()
    )

# Parse date
enrolment_clean["date"] = pd.to_datetime(
    enrolment_clean["date"],
    errors="coerce"
)

# Drop invalid geography
enrolment_clean = enrolment_clean.dropna(
    subset=["state", "district"]
)

# Age columns
age_cols = ["age_0_5", "age_5_17", "age_18_greater"]

# Handle missing values
enrolment_clean[age_cols] = enrolment_clean[age_cols].fillna(0)

# Remove negative values
for col in age_cols:
    enrolment_clean = enrolment_clean[
        enrolment_clean[col] >= 0
    ]

# Derived metric
enrolment_clean["enrolment_count"] = (
    enrolment_clean[age_cols].sum(axis=1)
)

# Deduplication
before = enrolment_clean.shape[0]
enrolment_clean = enrolment_clean.drop_duplicates()
after = enrolment_clean.shape[0]

print(f"Duplicates removed: {before - after}")


# Save cleaned data

enrolment_clean.to_csv(
    PROCESSED_DIR / "enrolment_clean.csv",
    index=False
)


# Daily district aggregation

enrolment_agg = (
    enrolment_clean
    .groupby(["date", "state", "district"], as_index=False)
    .agg({
        "age_0_5": "sum",
        "age_5_17": "sum",
        "age_18_greater": "sum",
        "enrolment_count": "sum"
    })
)

enrolment_agg.to_csv(
    PROCESSED_DIR / "enrolment_agg.csv",
    index=False
)

# Final sanity check
dup_count = enrolment_agg.duplicated(
    ["date", "state", "district"]
).sum()

print("Duplicate rows after aggregation:", dup_count)
print("✅ Enrolment cleaning & aggregation completed successfully")

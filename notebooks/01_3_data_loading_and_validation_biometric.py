import pandas as pd
import glob
from pathlib import Path


# Resolve project paths safely (independent of working directory)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "biometric"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Ensure output directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# Load raw biometric data

files = glob.glob(str(RAW_DIR / "*.csv"))

if not files:
    raise RuntimeError(f"No biometric raw files found in {RAW_DIR}")

df_list = [pd.read_csv(f) for f in files]
biometric_df = pd.concat(df_list, ignore_index=True)

print("Total rows:", biometric_df.shape[0])
print("Total columns:", biometric_df.shape[1])


# Cleaning

biometric_clean = biometric_df.copy()

# Standardise text fields
text_cols = ["state", "district"]
for col in text_cols:
    biometric_clean[col] = (
        biometric_clean[col]
        .astype(str)
        .str.strip()
        .str.title()
    )

# Parse date
biometric_clean["date"] = pd.to_datetime(
    biometric_clean["date"],
    errors="coerce"
)

# Drop invalid geography
biometric_clean = biometric_clean.dropna(
    subset=["state", "district"]
)

# Biometric age columns
bio_cols = ["bio_age_5_17", "bio_age_17_"]

# Handle missing values
biometric_clean[bio_cols] = biometric_clean[bio_cols].fillna(0)

# Remove negative values
for col in bio_cols:
    biometric_clean = biometric_clean[
        biometric_clean[col] >= 0
    ]

# Derived metric
biometric_clean["biometric_count"] = (
    biometric_clean["bio_age_5_17"] +
    biometric_clean["bio_age_17_"]
)

# Deduplication
before = biometric_clean.shape[0]
biometric_clean = biometric_clean.drop_duplicates()
after = biometric_clean.shape[0]

print("Duplicates removed:", before - after)


# Save cleaned data

biometric_clean.to_csv(
    PROCESSED_DIR / "biometric_clean.csv",
    index=False
)


# Daily district aggregation

biometric_agg = (
    biometric_clean
    .groupby(["date", "state", "district"], as_index=False)
    .agg({
        "bio_age_5_17": "sum",
        "bio_age_17_": "sum",
        "biometric_count": "sum"
    })
)

biometric_agg.to_csv(
    PROCESSED_DIR / "biometric_agg.csv",
    index=False
)

# Final sanity check
dup_count = biometric_agg.duplicated(
    ["date", "state", "district"]
).sum()

print("Duplicate rows after aggregation:", dup_count)
print("✅ Biometric cleaning & aggregation completed successfully")

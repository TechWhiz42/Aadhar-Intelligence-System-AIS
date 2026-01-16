import pandas as pd
import glob
from pathlib import Path


# Resolve project paths safely (independent of working directory)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "demographic"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# Ensure output directory exists
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# Load raw demographic data

files = glob.glob(str(RAW_DIR / "*.csv"))

if not files:
    raise RuntimeError(f"No demographic raw files found in {RAW_DIR}")

df_list = [pd.read_csv(f) for f in files]
demographic_df = pd.concat(df_list, ignore_index=True)

print("Total rows:", demographic_df.shape[0])
print("Total columns:", demographic_df.shape[1])


# Cleaning

demographic_clean = demographic_df.copy()

# Standardise text fields
text_cols = ["state", "district"]
for col in text_cols:
    demographic_clean[col] = (
        demographic_clean[col]
        .astype(str)
        .str.strip()
        .str.title()
    )

# Parse date
demographic_clean["date"] = pd.to_datetime(
    demographic_clean["date"],
    errors="coerce"
)

# Drop invalid geography
demographic_clean = demographic_clean.dropna(
    subset=["state", "district"]
)

# demographic age columns
demo_cols = ["demo_age_5_17", "demo_age_17_"]

# Handle missing values
demographic_clean[demo_cols] = demographic_clean[demo_cols].fillna(0)

# Remove negative values
for col in demo_cols:
    demographic_clean = demographic_clean[
        demographic_clean[col] >= 0
    ]

# Derived metric
demographic_clean["demographic_count"] = (
    demographic_clean["demo_age_5_17"] +
    demographic_clean["demo_age_17_"]
)

# Deduplication
before = demographic_clean.shape[0]
demographic_clean = demographic_clean.drop_duplicates()
after = demographic_clean.shape[0]

print("Duplicates removed:", before - after)


# Save cleaned data

demographic_clean.to_csv(
    PROCESSED_DIR / "demographic_clean.csv",
    index=False
)


# Daily district aggregation

demographic_agg = (
    demographic_clean
    .groupby(["date", "state", "district"], as_index=False)
    .agg({
        "demo_age_5_17": "sum",
        "demo_age_17_": "sum",
        "demographic_count": "sum"
    })
)

demographic_agg.to_csv(
    PROCESSED_DIR / "demographic_agg.csv",
    index=False
)

# Final sanity check
dup_count = demographic_agg.duplicated(
    ["date", "state", "district"]
).sum()

print("Duplicate rows after aggregation:", dup_count)
print("✅ demographic cleaning & aggregation completed successfully")

import subprocess
import sys

def run_step(name, command):
    print(f"\n STARTING: {name}")
    result = subprocess.run(command, shell=True)

    if result.returncode != 0:
        print(f"\n FAILED: {name}")
        sys.exit(1)

    print(f" COMPLETED: {name}")


def main():
    # ---------- INGESTION ----------

    run_step(
        "API Ingestion – Enrolment",
        "python src/api_ingestion/enrolment_api.py --next-batch"
    )

    run_step(
        "API Ingestion – Biometric",
        "python src/api_ingestion/biometric_api.py --next-batch"
    )

    run_step(
        "API Ingestion – Demographic",
        "python src/api_ingestion/demographic_api.py --next-batch"
    )
    # ---------- CLEANING ----------
    run_step(
        "Clean Enrolment Data",
        "python notebooks/01_2_data_loading_and_validation_enrolment.py"
    )

    run_step(
        "Clean Biometric Data",
        "python notebooks/01_3_data_loading_and_validation_biometric.py"
    )

    run_step(
        "Clean Demographic Data",
        "python notebooks/01_data_loading_and_validation_demographic.py"
    )

    # ---------- DAILY MERGE ----------
    run_step(
        "Merge Daily Aggregates",
        "python notebooks/08_merge_aggregated_data.py"
    )

    # ---------- DAILY FEATURES ----------
    run_step(
        "Daily Feature Engineering",
        "python notebooks/05_feature_engineering.py"
    )

    # ---------- MONTHLY ----------
    run_step(
        "Monthly Aggregation",
        "python notebooks/09_aggregate_monthly.py"
    )

    run_step(
        "Monthly Feature Engineering",
        "python notebooks/10_feature_engineering_monthly.py"
    )

    # ---------- MODEL ----------
    run_step(
        "Anomaly Detection Model",
        "python notebooks/07_predictive_model.py"
    )

    print("\n PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()

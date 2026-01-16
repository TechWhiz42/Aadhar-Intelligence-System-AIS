import os
from dotenv import load_dotenv
import argparse
from pathlib import Path
from base_ingestion import BaseIngestion

load_dotenv()

API_URL = "https://api.data.gov.in/resource/ecd49b12-3084-4521-8f7e-ca8bf72069ba"
API_KEY = os.getenv("DATA_GOV_API_KEY")

if not API_KEY:
    raise RuntimeError("DATA_GOV_API_KEY not found in environment variable")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "enrolment"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--next-batch", action="store_true")
    parser.add_argument("--auto", action="store_true")

    args = parser.parse_args()

    ingestor = BaseIngestion(
        api_url=API_URL,
        api_key=API_KEY,
        output_dir=OUTPUT_DIR
    )

    if args.auto:
        ingestor.auto_ingest()

    if args.next_batch:
        ingestor.ingest_next_batch()

    if not (args.auto or args.next_batch):
        parser.print_help()


if __name__ == "__main__":
    main()

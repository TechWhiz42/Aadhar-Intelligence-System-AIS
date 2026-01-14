import os
from dotenv import load_dotenv
import argparse
from pathlib import Path
from base_ingestion import BaseIngestion

load_dotenv()

API_URL = "https://api.data.gov.in/resource/19eac040-0b94-49fa-b239-4f2fd8677d53"
API_KEY = os.getenv("DATA_GOV_API_KEY")

if not API_KEY:
    raise RuntimeError("DATA_GOV_API_KEY not found in environment variable")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "demographic"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--next-batch", action="store_true")
    parser.add_argument("--auto", action="store_true")

    args = parser.parse_args()

    ingestor = BaseIngestion(API_URL, API_KEY, OUTPUT_DIR)

    if args.auto:
        ingestor.auto_ingest()

    if args.next_batch:
        ingestor.ingest_next_batch()

    if not (args.auto or args.next_batch):
        parser.print_help()


if __name__ == "__main__":
    main()

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import time
from requests.exceptions import ReadTimeout, ConnectionError

LIMIT = 1000
BATCH_SIZE = 200000
SLEEP_SECONDS = 0.5
MAX_RETRIES = 5


class BaseIngestion:
    def __init__(self, api_url, api_key, output_dir):
        self.api_url = api_url
        self.api_key = api_key
        self.output_dir = output_dir

        self.output_dir.mkdir(parents=True ,exist_ok=True)

        self.raw_file = self.output_dir / "raw.csv"
        self.progress_file = self.output_dir / "last_offset.txt"

    def load_last_offset(self):
        if self.progress_file.exists():
            return int(self.progress_file.read_text())
        return 0

    def save_offset(self, offset):
        self.progress_file.write_text(str(offset))

    def fetch_page(self, offset):
        params = {
            "api-key": self.api_key,
            "format": "xml",
            "limit": LIMIT,
            "offset": offset
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    self.api_url,
                    params=params,
                    timeout=(10, 60)
                )
                response.raise_for_status()
                return ET.fromstring(response.text)

            except (ReadTimeout, ConnectionError):
                time.sleep(5 * attempt)

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code in (429, 500, 502, 503, 504):
                    time.sleep(5 * attempt)
                else:
                    raise

        raise RuntimeError("API failed after retries")

    def ingest_next_batch(self):
        offset = self.load_last_offset()
        fetched = 0

        print(f"\n▶ Starting ingestion batch")
        print(f"▶ Resuming from offset: {offset}")
        print(f"▶ Target batch size: {BATCH_SIZE}\n")

        while fetched < BATCH_SIZE:
            print(f"📡 Fetching offset {offset} ...")

            root = self.fetch_page(offset)
            records = root.findall(".//records/item")

            if not records:
                print("⛔ No more records returned by API.")
                break

            rows = [{child.tag: child.text for child in record} for record in records]
            df = pd.DataFrame(rows)

            remaining = BATCH_SIZE - fetched
            if len(df) > remaining:
                df = df.iloc[:remaining]

            write_header = (
                    not self.raw_file.exists()
                    or self.raw_file.stat().st_size == 0
            )

            df.to_csv(
                self.raw_file,
                mode="a",
                index=False,
                header=write_header
            )

            count = len(df)
            fetched += count
            offset += count
            self.save_offset(offset)

            print(
                f"✅ Fetched {count} rows | "
                f"Batch total: {fetched}/{BATCH_SIZE} | "
                f"Next offset: {offset}"
            )

            time.sleep(SLEEP_SECONDS)

        print("\n✔ Batch completed successfully")
        print(f"✔ Rows fetched in this run: {fetched}")
        print(f"✔ Saved offset: {offset}\n")

        return fetched

    def auto_ingest(self):
        while True:
            before = self.load_last_offset()
            self.ingest_next_batch()
            after = self.load_last_offset()

            if after == before:
                break

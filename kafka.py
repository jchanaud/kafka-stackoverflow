import requests
import csv
from datetime import datetime, timedelta
import os

BASE_URL = "https://api.stackexchange.com/2.3/questions"
PARAMS_TEMPLATE = {
    "pagesize": 100,
    "order": "asc",
    "sort": "creation",
    "tagged": "kafka",
    "site": "stackoverflow",
    "filter": "!)riR7Z8z9CJE7wdL)i*K"
}

# Docs
# https://api.stackexchange.com/docs/questions#page=1&pagesize=100&fromdate=2020-01-01&todate=2025-08-31&order=desc&sort=activity&tagged=kafka&filter=!)riR7Z8z9CJE7wdL)i*K&site=stackoverflow&run=true

# Default global range
START_DATE = datetime(2020, 1, 1)
END_DATE   = datetime(2025, 8, 31)

CHUNK_SIZE = timedelta(days=30)
PROGRESS_FILE = "progress.txt"
OUTPUT_FILE = "stackoverflow_kafka.csv"

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            ts = int(f.read().strip())
            return datetime.utcfromtimestamp(ts)
    return START_DATE

def save_progress(timestamp):
    with open(PROGRESS_FILE, "w") as f:
        f.write(str(timestamp))

def fetch_and_write(writer, from_ts, to_ts, last_written):
    page = 1
    while True:
        params = PARAMS_TEMPLATE.copy()
        params.update({
            "fromdate": int(from_ts.timestamp()),
            "todate": int(to_ts.timestamp()),
            "page": page
        })
        resp = requests.get(BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

        for item in data.get("items", []):
            creation_dt = datetime.utcfromtimestamp(item["creation_date"])
            date_str = creation_dt.strftime("%Y-%m-%d %H:%M:%S")
            tags_str = ",".join(item["tags"])
            title = item["title"].replace("&#39;", "'")
            writer.writerow([date_str, tags_str, title])
            last_written = creation_dt
            save_progress(item["creation_date"])  # update checkpoint

        if not data.get("has_more"):
            break

        page += 1

    return last_written

if __name__ == "__main__":
    # resume from last saved progress if available
    chunk_start = load_progress()
    last_written = chunk_start

    # open file (append if resuming)
    file_exists = os.path.exists(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        if not file_exists:
            writer.writerow(["date", "tags", "question"])  # header

        while chunk_start < END_DATE:
            chunk_end = min(chunk_start + CHUNK_SIZE, END_DATE)
            print(f"Fetching {chunk_start.date()} → {chunk_end.date()}")
            last_written = fetch_and_write(writer, chunk_start, chunk_end, last_written)
            chunk_start = chunk_end  # move to next chunk

    print(f"✅ Finished. Last saved timestamp: {last_written} ({int(last_written.timestamp())})")

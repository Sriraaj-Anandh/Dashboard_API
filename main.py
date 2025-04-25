from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
from datetime import datetime
from collections import defaultdict
import pymysql
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Cloud MySQL config from environment variables
CLOUD_MYSQL_CONFIG = {
    'host': os.getenv("MYSQL_HOST"),
    'user': os.getenv("MYSQL_USER"),
    'password': os.getenv("MYSQL_PASSWORD"),
    'database': os.getenv("MYSQL_DATABASE"),
    'cursorclass': pymysql.cursors.DictCursor
}

app = FastAPI()

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Modify this for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_all_metrics():
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM update_metrics")
            return cursor.fetchall()
    finally:
        conn.close()

@app.get("/metrics")
def get_all_metrics() -> Dict:
    raw_metrics = fetch_all_metrics()

    summary = {
        "total_updates": 0,
        "updates_per_day": defaultdict(int),
        "updates_per_month": defaultdict(int),
        "top_user": None,
        "top_user_count": 0,
        "total_users": 0,
        "table_wise_metrics": defaultdict(lambda: {"count": 0, "last_updated": None})
    }

    user_counter = defaultdict(int)

    for row in raw_metrics:
        ts = row["timestamp"]
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        date = ts.date()
        month_key = ts.strftime("%Y-%m")

        summary["total_updates"] += row["update_count"]
        summary["updates_per_day"][str(date)] += row["update_count"]
        summary["updates_per_month"][month_key] += row["update_count"]

        if row["top_user"]:
            user_counter[row["top_user"]] += row["top_user_count"]

        table = row["table_name"]
        summary["table_wise_metrics"][table]["count"] += row["update_count"]
        last_updated = row["last_updated"]
        if last_updated and (
            summary["table_wise_metrics"][table]["last_updated"] is None or
            last_updated > summary["table_wise_metrics"][table]["last_updated"]
        ):
            summary["table_wise_metrics"][table]["last_updated"] = last_updated

    if user_counter:
        summary["top_user"] = max(user_counter, key=user_counter.get)
        summary["top_user_count"] = user_counter[summary["top_user"]]

    if raw_metrics:
        summary["total_users"] = raw_metrics[-1]["total_users"]

    # Convert defaultdicts to regular dicts for JSON response
    summary["updates_per_day"] = dict(summary["updates_per_day"])
    summary["updates_per_month"] = dict(summary["updates_per_month"])
    summary["table_wise_metrics"] = dict(summary["table_wise_metrics"])

    return summary

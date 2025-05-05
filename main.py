from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
from datetime import datetime
from collections import defaultdict
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CLOUD_MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'cursorclass': pymysql.cursors.DictCursor
}

def fetch_all_metrics():
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM update_metrics")
            return cursor.fetchall()
    finally:
        conn.close()

# Core summary builder
def build_summary():
    raw_metrics = fetch_all_metrics()

    summary = {
        "total_updates": 0,
        "updates_per_day": defaultdict(int),
        "updates_per_month": defaultdict(int),
        "updates_per_weekday": defaultdict(int),
        "top_user": None,
        "top_user_count": 0,
        "total_users": 0,
        "table_wise_metrics": defaultdict(lambda: {"count": 0, "last_updated": None})
    }

    user_counter = defaultdict(int)

    for row in raw_metrics:
        ts = row.get("timestamp") or row.get("detected_timestamp")
        if isinstance(ts, str):
            ts = datetime.fromisoformat(ts)
        date = ts.date()
        month_key = ts.strftime("%Y-%m")
        weekday = ts.strftime("%A")

        summary["total_updates"] += row["update_count"]
        summary["updates_per_day"][str(date)] += row["update_count"]
        summary["updates_per_month"][month_key] += row["update_count"]
        summary["updates_per_weekday"][weekday] += row["update_count"]

        if row["top_user"]:
            user_counter[row["top_user"]] += row["top_user_count"]

        table = row["table_name"]
        summary["table_wise_metrics"][table]["count"] += row["update_count"]
        last_updated = row.get("last_updated")
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

    return summary


# -------------------- API Endpoints --------------------

@app.get("/metrics")
def get_all_metrics() -> Dict:
    summary = build_summary()
    summary["updates_per_day"] = dict(summary["updates_per_day"])
    summary["updates_per_month"] = dict(summary["updates_per_month"])
    summary["updates_per_weekday"] = dict(summary["updates_per_weekday"])
    summary["table_wise_metrics"] = dict(summary["table_wise_metrics"])
    return summary

@app.get("/metrics/top-user")
def get_top_user():
    summary = build_summary()
    return {"top_user": summary["top_user"], "entry_count": summary["top_user_count"]}

@app.get("/metrics/total-updates")
def get_total_updates():
    summary = build_summary()
    return {"total_updates": summary["total_updates"]}

@app.get("/metrics/weekday")
def get_updates_per_day():
    summary = build_summary()
    return summary["updates_per_day"]

@app.get("/metrics/monthly")
def get_updates_per_month():
    summary = build_summary()
    return summary["updates_per_month"]

@app.get("/metrics/total-users")
def get_total_users():
    summary = build_summary()
    return {"total_users": summary["total_users"]}

@app.get("/metrics/per-day")
def get_metrics_per_day():
    summary = build_summary()
    return {"per_day": summary["updates_per_day"]}

@app.get("/metrics/per-month")
def get_metrics_per_month():
    summary = build_summary()
    return {"per_month": summary["updates_per_month"]}

@app.get("/metrics/per-weekday")
def get_metrics_per_weekday():
    summary = build_summary()
    return {"per_weekday": summary["updates_per_weekday"]}

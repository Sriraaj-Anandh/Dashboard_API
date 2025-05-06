from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
from typing import Dict
from datetime import datetime
import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cloud MySQL config
CLOUD_MYSQL_CONFIG = {
    'host': os.getenv('CLOUD_DB_HOST'),
    'user': os.getenv('CLOUD_DB_USER'),
    'password': os.getenv('CLOUD_DB_PASSWORD'),
    'database': os.getenv('CLOUD_DB_NAME'),
    'cursorclass': pymysql.cursors.DictCursor
}

def fetch_metrics_for_project(project_name: str):
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM update_metrics WHERE project_name = %s", (project_name,))
            return cursor.fetchall()
    finally:
        conn.close()

def get_all_projects():
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT project_name FROM update_metrics")
            return [row['project_name'] for row in cursor.fetchall()]
    finally:
        conn.close()

def get_tables_for_project(project_name: str):
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT table_name FROM update_metrics WHERE project_name = %s", (project_name,))
            return [row['table_name'] for row in cursor.fetchall()]
    finally:
        conn.close()

def build_summary(metrics):
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

    for row in metrics:
        ts = row.get("timestamp") or row.get("detected_timestamp")
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
        last_updated = row.get("last_updated")
        if last_updated and (
            summary["table_wise_metrics"][table]["last_updated"] is None or
            last_updated > summary["table_wise_metrics"][table]["last_updated"]
        ):
            summary["table_wise_metrics"][table]["last_updated"] = last_updated

    if user_counter:
        summary["top_user"] = max(user_counter, key=user_counter.get)
        summary["top_user_count"] = user_counter[summary["top_user"]]

    if metrics:
        summary["total_users"] = metrics[-1]["total_users"]

    return summary

# --- ENDPOINTS ---

@app.get("/projects")
def list_projects():
    return {"projects": get_all_projects()}

@app.get("/projects/{project_name}/tables")
def list_tables(project_name: str):
    tables = get_tables_for_project(project_name)
    if not tables:
        raise HTTPException(status_code=404, detail="Project not found")
    return {"project": project_name, "tables": tables}

@app.get("/projects/{project_name}/metrics")
def get_project_metrics(project_name: str):
    metrics = fetch_metrics_for_project(project_name)
    if not metrics:
        raise HTTPException(status_code=404, detail="Project not found")
    summary = build_summary(metrics)
    return {
        **summary,
        "updates_per_day": dict(summary["updates_per_day"]),
        "updates_per_month": dict(summary["updates_per_month"]),
        "table_wise_metrics": dict(summary["table_wise_metrics"])
    }

@app.get("/projects/{project_name}/metrics/top-user")
def get_project_top_user(project_name: str):
    metrics = fetch_metrics_for_project(project_name)
    summary = build_summary(metrics)
    return {"top_user": summary["top_user"], "entry_count": summary["top_user_count"]}

@app.get("/projects/{project_name}/metrics/total-updates")
def get_project_total_updates(project_name: str):
    metrics = fetch_metrics_for_project(project_name)
    summary = build_summary(metrics)
    return {"total_updates": summary["total_updates"]}

@app.get("/projects/{project_name}/metrics/weekday")
def get_project_weekday_metrics(project_name: str):
    metrics = fetch_metrics_for_project(project_name)
    summary = build_summary(metrics)
    return summary["updates_per_day"]

@app.get("/projects/{project_name}/metrics/monthly")
def get_project_monthly_metrics(project_name: str):
    metrics = fetch_metrics_for_project(project_name)
    summary = build_summary(metrics)
    return summary["updates_per_month"]

@app.get("/projects/{project_name}/metrics/total-users")
def get_project_total_users(project_name: str):
    metrics = fetch_metrics_for_project(project_name)
    summary = build_summary(metrics)
    return {"total_users": summary["total_users"]}

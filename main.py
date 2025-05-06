import os
import logging
from dotenv import load_dotenv
from fastapi import FastAPI
import pymysql
from typing import List

# Load environment variables
load_dotenv()

# Logging
logging.basicConfig(level=logging.DEBUG)

# MySQL configuration
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Use DictCursor for dictionary-style rows
CLOUD_MYSQL_CONFIG = {
    "host": MYSQL_HOST,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DATABASE,
    "port": MYSQL_PORT,
    "cursorclass": pymysql.cursors.DictCursor
}

app = FastAPI()

# Utility function to get table_name from projects table
def get_metrics_table(project_id: int) -> str:
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM projects WHERE project_id = %s", (project_id,))
            result = cursor.fetchone()
            return result["table_name"] if result else None
    finally:
        conn.close()

# Endpoint: /projects
@app.get("/projects")
async def list_projects():
    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM projects")
            projects = cursor.fetchall()
            return {"projects": projects}
    except Exception as e:
        logging.error(f"Error fetching projects: {e}")
        return {"error": "Failed to fetch projects"}
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}
@app.get("/metrics/{project_id}")
async def get_project_metrics(project_id: int):
    table_name = get_metrics_table(project_id)
    if not table_name:
        return {"error": "Project not found"}

    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            query = f"SELECT * FROM `{table_name}` ORDER BY detected_timestamp DESC LIMIT 1"
            cursor.execute(query)
            result = cursor.fetchone()
            return {"metrics": result} if result else {"error": "No data found"}
    except Exception as e:
        logging.error(f"Error fetching metrics: {e}")
        return {"error": "Failed to fetch metrics"}
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/total-users
@app.get("/metrics/{project_id}/total-users")
async def get_total_users(project_id: int):
    table_name = get_metrics_table(project_id)
    if not table_name:
        return {"error": "Project not found"}

    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT total_users FROM `{table_name}` ORDER BY detected_timestamp DESC LIMIT 1")
            result = cursor.fetchone()
            return {"total_users": result["total_users"]} if result else {"total_users": 0}
    except Exception as e:
        logging.error(f"Error fetching total users: {e}")
        return {"error": "Failed to fetch total users"}
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/top-user
@app.get("/metrics/{project_id}/top-user")
async def get_top_user(project_id: int):
    table_name = get_metrics_table(project_id)
    if not table_name:
        return {"error": "Project not found"}

    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT top_user, top_user_count FROM `{table_name}` ORDER BY detected_timestamp DESC LIMIT 1")
            result = cursor.fetchone()
            return {"top_user": result["top_user"], "entry_count": result["top_user_count"]} if result else {"top_user": None, "entry_count": 0}
    except Exception as e:
        logging.error(f"Error fetching top user: {e}")
        return {"error": "Failed to fetch top user"}
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/entries-per-day
@app.get("/metrics/{project_id}/entries-per-day")
async def get_entries_per_day(project_id: int):
    table_name = get_metrics_table(project_id)
    if not table_name:
        return {"error": "Project not found"}

    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT day, SUM(update_count) AS total_updates
                FROM `{table_name}`
                GROUP BY day
                ORDER BY day
            """)
            result = cursor.fetchall()
            return {"entries_per_day": result}
    except Exception as e:
        logging.error(f"Error fetching entries per day: {e}")
        return {"error": "Failed to fetch daily entries"}
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/entries-per-weekday
@app.get("/metrics/{project_id}/entries-per-weekday")
async def get_entries_per_weekday(project_id: int):
    table_name = get_metrics_table(project_id)
    if not table_name:
        return {"error": "Project not found"}

    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT weekday, SUM(update_count) AS total_updates
                FROM `{table_name}`
                GROUP BY weekday
                ORDER BY FIELD(weekday, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday')
            """)
            result = cursor.fetchall()
            return {"entries_per_weekday": result}
    except Exception as e:
        logging.error(f"Error fetching entries per weekday: {e}")
        return {"error": "Failed to fetch weekday entries"}
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/entries-per-month
@app.get("/metrics/{project_id}/entries-per-month")
async def get_entries_per_month(project_id: int):
    table_name = get_metrics_table(project_id)
    if not table_name:
        return {"error": "Project not found"}

    conn = pymysql.connect(**CLOUD_MYSQL_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT month, SUM(update_count) AS total_updates
                FROM `{table_name}`
                GROUP BY month
                ORDER BY FIELD(month, 'January','February','March','April','May','June','July','August','September','October','November','December')
            """)
            result = cursor.fetchall()
            return {"entries_per_month": result}
    except Exception as e:
        logging.error(f"Error fetching entries per month: {e}")
        return {"error": "Failed to fetch monthly entries"}
    finally:
        conn.close()

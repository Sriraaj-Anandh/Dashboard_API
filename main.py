import logging
import re
import time
from fastapi import FastAPI, Query, HTTPException
import pymysql
from datetime import datetime, timedelta
from typing import List
from pydantic import BaseModel
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# MySQL configuration
MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE"),
    "port": int(os.getenv("MYSQL_PORT", 3306)),
    "cursorclass": pymysql.cursors.DictCursor
}

# Initialize FastAPI
app = FastAPI(title="Report System Dashboard API")

# Pydantic models
class Metrics(BaseModel):
    id: int
    project_name: str | None
    table_name: str | None
    update_count: int
    top_user: int | None
    top_user_count: int | None
    total_users: int | None
    detected_timestamp: datetime | None
    last_updated: datetime | None

class AggregatedMetrics(BaseModel):
    total_update_count: int
    top_user: int | None
    top_user_count: int | None
    total_users: int | None

class EntryPerDay(BaseModel):
    day: str
    total_updates: int

class EntryPerWeekday(BaseModel):
    weekday: str
    total_updates: int

class EntryPerMonth(BaseModel):
    month: str
    total_updates: int

class TopUser(BaseModel):
    top_user: int | None
    entry_count: int

class Project(BaseModel):
    project_id: int
    project_name: str
    table_name: str

# Utility function to connect to MySQL with retry
def connect_mysql():
    config = MYSQL_CONFIG.copy()
    for attempt in range(3):
        try:
            conn = pymysql.connect(**config)
            logger.debug("MySQL connection established")
            return conn
        except pymysql.MySQLError as e:
            logger.error(f"MySQL connection attempt {attempt + 1} failed: {e}")
            time.sleep(2)
    raise HTTPException(status_code=500, detail="MySQL connection failed after retries")

# Utility to validate and sanitize table_name
def sanitize_table_name(table_name: str) -> str:
    if not table_name or not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        logger.error(f"Invalid table name: {table_name}")
        raise HTTPException(status_code=400, detail="Invalid table name")
    return table_name

# Utility to get table_name from projects table
def get_metrics_table(project_id: int) -> str:
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT table_name FROM projects WHERE project_id = %s", (project_id,))
            result = cursor.fetchone()
            if not result:
                logger.error(f"No table found for project_id {project_id}")
                raise HTTPException(status_code=404, detail="Project not found")
            table_name = sanitize_table_name(result["table_name"])
            logger.debug(f"Table name for project_id {project_id}: {table_name}")
            return table_name
    except pymysql.MySQLError as e:
        logger.error(f"Error fetching table_name for project_id {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# Utility to format DD/MM/YYYY to YYYY-MM-DD
def format_date(date_str: str) -> str:
    try:
        date_obj = datetime.strptime(date_str, "%d/%m/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use DD/MM/YYYY.")

# Endpoint: /projects
@app.get("/projects", response_model=dict)
async def list_projects():
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT project_id, project_name, table_name FROM projects")
            projects = cursor.fetchall()
            logger.debug(f"Projects: {projects}")
            return {"projects": projects}
    except pymysql.MySQLError as e:
        logger.error(f"Error fetching projects: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch projects: {str(e)}")
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}
@app.get("/metrics/{project_id}", response_model=dict)
async def get_project_metrics(project_id: int):
    table_name = get_metrics_table(project_id)
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            query = f"""
                SELECT id, project_name, table_name, update_count,
                       top_user, top_user_count, total_users,
                       detected_timestamp, last_updated
                FROM `{table_name}`
                ORDER BY last_updated DESC LIMIT 1
            """
            cursor.execute(query)
            result = cursor.fetchone()
            if not result:
                logger.warning(f"No metrics found for table {table_name}")
                raise HTTPException(status_code=404, detail="No metrics found")
            logger.debug(f"Latest metrics for project {project_id}: {result}")
            return {"metrics": result}
    except pymysql.ProgrammingError as e:
        logger.error(f"Query error for project {project_id}, table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
    except pymysql.MySQLError as e:
        logger.error(f"Database error for project {project_id}, table {table_name}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/by-date
@app.get("/metrics/{project_id}/by-date", response_model=dict)
async def get_metrics_by_date(project_id: int, date: str = Query(..., description="Date in DD/MM/YYYY format")):
    try:
        formatted_date = format_date(date)
        table_name = get_metrics_table(project_id)
        conn = connect_mysql()
        try:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT
                        SUM(update_count) AS total_update_count,
                        MAX(top_user) AS top_user,
                        MAX(top_user_count) AS top_user_count,
                        MAX(total_users) AS total_users
                    FROM `{table_name}`
                    WHERE DATE(last_updated) = %s
                """
                cursor.execute(query, (formatted_date,))
                result = cursor.fetchone()
                if not result or result["total_update_count"] is None:
                    logger.warning(f"No metrics found for {formatted_date} in table {table_name}")
                    raise HTTPException(status_code=404, detail=f"No metrics found for {formatted_date}")
                logger.debug(f"Metrics for project {project_id} on {formatted_date}: {result}")
                return {"metrics": result}
        except pymysql.ProgrammingError as e:
            logger.error(f"Query error for project {project_id} by date: {e}")
            raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
        except pymysql.MySQLError as e:
            logger.error(f"Database error for project {project_id} by date: {e}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        finally:
            conn.close()
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error for project {project_id} by date: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# Endpoint: /metrics/{project_id}/today
@app.get("/metrics/{project_id}/today", response_model=dict)
async def get_metrics_today(project_id: int):
    try:
        today_date = datetime.today().strftime("%Y-%m-%d")
        table_name = get_metrics_table(project_id)
        conn = connect_mysql()
        try:
            with conn.cursor() as cursor:
                query = f"""
                    SELECT
                        SUM(update_count) AS total_update_count,
                        MAX(top_user) AS top_user,
                        MAX(top_user_count) AS top_user_count,
                        MAX(total_users) AS total_users
                    FROM `{table_name}`
                    WHERE DATE(last_updated) = %s
                """
                cursor.execute(query, (today_date,))
                result = cursor.fetchone()
                if not result or result["total_update_count"] is None:
                    logger.warning(f"No metrics found for today in table {table_name}")
                    raise HTTPException(status_code=404, detail="No metrics found for today")
                logger.debug(f"Today's metrics for project {project_id}: {result}")
                return {"metrics": result}
        except pymysql.ProgrammingError as e:
            logger.error(f"Query error for today's metrics for project {project_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
        except pymysql.MySQLError as e:
            logger.error(f"Database error for today's metrics for project {project_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        finally:
            conn.close()
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Unexpected error for today's metrics for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

# Endpoint: /metrics/{project_id}/total-users
@app.get("/metrics/{project_id}/total-users", response_model=dict)
async def get_total_users(project_id: int):
    table_name = get_metrics_table(project_id)
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            query = f"SELECT total_users FROM `{table_name}` ORDER BY last_updated DESC LIMIT 1"
            cursor.execute(query)
            result = cursor.fetchone()
            if not result:
                logger.warning(f"No total users found for table {table_name}")
                raise HTTPException(status_code=404, detail="No total users found")
            logger.debug(f"Total users for project {project_id}: {result}")
            return {"total_users": result["total_users"] or 0}
    except pymysql.ProgrammingError as e:
        logger.error(f"Query error for total users for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
    except pymysql.MySQLError as e:
        logger.error(f"Database error for total users for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/top-user
@app.get("/metrics/{project_id}/top-user", response_model=TopUser)
async def get_top_user(project_id: int):
    table_name = get_metrics_table(project_id)
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            query = f"SELECT top_user, top_user_count FROM `{table_name}` ORDER BY last_updated DESC LIMIT 1"
            cursor.execute(query)
            result = cursor.fetchone()
            if not result:
                logger.warning(f"No top user found for table {table_name}")
                raise HTTPException(status_code=404, detail="No top user found")
            logger.debug(f"Top user for project {project_id}: {result}")
            return {
                "top_user": result["top_user"],
                "entry_count": result["top_user_count"] or 0
            }
    except pymysql.ProgrammingError as e:
        logger.error(f"Query error for top user for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
    except pymysql.MySQLError as e:
        logger.error(f"Database error for top user for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/entries-per-day
@app.get("/metrics/{project_id}/entries-per-day", response_model=dict)
async def get_entries_per_day(project_id: int):
    table_name = get_metrics_table(project_id)
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            query = f"""
                SELECT DATE(last_updated) AS day, SUM(update_count) AS total_updates
                FROM `{table_name}`
                WHERE last_updated >= CURDATE() - INTERVAL 30 DAY
                GROUP BY DATE(last_updated)
                ORDER BY DATE(last_updated)
            """
            cursor.execute(query)
            result = cursor.fetchall()
            logger.debug(f"Daily entries for project {project_id}: {result}")
            return {"entries_per_day": result}
    except pymysql.ProgrammingError as e:
        logger.error(f"Query error for daily entries for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
    except pymysql.MySQLError as e:
        logger.error(f"Database error for daily entries for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/entries-per-weekday
@app.get("/metrics/{project_id}/entries-per-weekday", response_model=dict)
async def get_entries_per_weekday(project_id: int):
    table_name = get_metrics_table(project_id)
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            query = f"""
                SELECT 
                    weekday AS weekday_name, 
                    SUM(update_count) AS total_updates
                FROM `{table_name}`
                WHERE last_updated >= CURDATE() - INTERVAL 30 DAY
                GROUP BY weekday
                ORDER BY FIELD(weekday, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')
            """
            cursor.execute(query)
            result = [{"weekday": row["weekday_name"], "total_updates": row["total_updates"]} for row in cursor.fetchall()]
            logger.debug(f"Weekday entries for project {project_id}: {result}")
            return {"entries_per_weekday": result}
    except pymysql.ProgrammingError as e:
        logger.error(f"Query error for weekday entries for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
    except pymysql.MySQLError as e:
        logger.error(f"Database error for weekday entries for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

# Endpoint: /metrics/{project_id}/entries-per-month
@app.get("/metrics/{project_id}/entries-per-month", response_model=dict)
async def get_entries_per_month(project_id: int):
    table_name = get_metrics_table(project_id)
    conn = connect_mysql()
    try:
        with conn.cursor() as cursor:
            query = f"""
                SELECT 
                    DATE_FORMAT(last_updated, '%M') AS month, 
                    SUM(update_count) AS total_updates
                FROM `{table_name}`
                WHERE last_updated >= CURDATE() - INTERVAL 6 MONTH
                GROUP BY DATE_FORMAT(last_updated, '%M')
                ORDER BY MIN(last_updated)
            """
            cursor.execute(query)
            result = cursor.fetchall()
            logger.debug(f"Monthly entries for project {project_id}: {result}")
            return {"entries_per_month": result}
    except pymysql.ProgrammingError as e:
        logger.error(f"Query error for monthly entries for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
    except pymysql.MySQLError as e:
        logger.error(f"Database error for monthly entries for project {project_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
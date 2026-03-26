import requests

# def get_weather(city):
#     api_key = "ef06781723b446725e6f3410b7565630"  # 🔹 Replace this with your actual OpenWeather API key
#     base_url = "https://api.openweathermap.org/data/2.5/weather"

#     params = {
#         "q": city,
#         "appid": api_key,
#         "units": "metric"  # For Celsius
#     }

#     response = requests.get(base_url, params=params)

#     if response.status_code == 200:
#         data = response.json()
#         print(f"Weather in {city}:\n")
#         print(f"🌡 Temperature: {data['main']['temp']}°C")
#         print(f"☁ Condition: {data['weather'][0]['description'].capitalize()}")
#         print(f"💧 Humidity: {data['main']['humidity']}%")
#         print(f"💨 Wind Speed: {data['wind']['speed']} m/s")
#     else:
#         print("City not found or error fetching data.")

# # Example usage
# #get_weather("Mumbai")
# get_weather("Pune")




"""
================================================================================
  WEATHER DATA ENGINEERING PIPELINE
  Author: [Your Name]
  Description: An ETL pipeline that ingests real-time weather data from the
               OpenWeatherMap API, transforms and validates it, stores it in
               a local SQLite data warehouse, and exposes summary analytics.
================================================================================
"""

import requests
import sqlite3
import json
import logging
import os
from datetime import datetime
from typing import Optional

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_KEY   = "ef06781723b446725e6f3410b7565630"   # Replace with your key
BASE_URL  = "https://api.openweathermap.org/data/2.5/weather"
DB_PATH   = "weather_warehouse.db"
LOG_FILE  = "pipeline.log"
CITIES    = ["Pune", "Mumbai", "Delhi", "Bengaluru", "Hyderabad"]

# ─── LOGGING SETUP ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 1 — EXTRACT
# ══════════════════════════════════════════════════════════════════════════════

def extract(city: str) -> Optional[dict]:
    """
    Extract raw weather data from OpenWeatherMap API.
    Returns raw JSON payload or None on failure.
    """
    logger.info(f"[EXTRACT] Fetching data for city: {city}")
    try:
        response = requests.get(
            BASE_URL,
            params={"q": city, "appid": API_KEY, "units": "metric"},
            timeout=10
        )
        response.raise_for_status()
        raw = response.json()
        logger.info(f"[EXTRACT] Success — HTTP {response.status_code}")
        return raw
    except requests.exceptions.HTTPError as e:
        logger.error(f"[EXTRACT] HTTP error for {city}: {e}")
    except requests.exceptions.ConnectionError:
        logger.error(f"[EXTRACT] Network error — check your connection.")
    except requests.exceptions.Timeout:
        logger.error(f"[EXTRACT] Request timed out for {city}.")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

def transform(raw: dict) -> Optional[dict]:
    """
    Transform and validate raw API payload into a clean, structured record.
    Applies business rules: unit conversions, derived fields, data quality checks.
    """
    logger.info("[TRANSFORM] Applying transformations & validations...")

    try:
        record = {
            # Identifiers & metadata
            "city"            : raw["name"],
            "country"         : raw["sys"]["country"],
            "ingested_at"     : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "data_source"     : "OpenWeatherMap API v2.5",

            # Core weather metrics
            "temp_celsius"    : round(raw["main"]["temp"], 2),
            "feels_like_c"    : round(raw["main"]["feels_like"], 2),
            "temp_min_c"      : round(raw["main"]["temp_min"], 2),
            "temp_max_c"      : round(raw["main"]["temp_max"], 2),
            "humidity_pct"    : raw["main"]["humidity"],
            "pressure_hpa"    : raw["main"]["pressure"],
            "wind_speed_mps"  : raw["wind"]["speed"],
            "wind_deg"        : raw["wind"].get("deg", None),
            "visibility_m"    : raw.get("visibility", None),
            "cloud_cover_pct" : raw["clouds"]["all"],
            "condition"       : raw["weather"][0]["main"],
            "condition_desc"  : raw["weather"][0]["description"].title(),
            "raw_payload"     : json.dumps(raw),   # store raw for reprocessing

            # ── Derived / Engineered Fields ──────────────────────────────────
            # Fahrenheit conversion
            "temp_fahrenheit" : round((raw["main"]["temp"] * 9/5) + 32, 2),

            # Heat index category (simple rule-based)
            "heat_category"   : _classify_heat(raw["main"]["temp"]),

            # Wind description
            "wind_label"      : _classify_wind(raw["wind"]["speed"]),
        }

        # ── Data Quality Checks ───────────────────────────────────────────────
        assert -90  <= record["temp_celsius"]   <= 60,  "Temp out of range"
        assert 0    <= record["humidity_pct"]   <= 100, "Humidity out of range"
        assert record["wind_speed_mps"]         >= 0,   "Negative wind speed"

        logger.info(f"[TRANSFORM] Record for {record['city']} passed validation ✓")
        return record

    except (KeyError, AssertionError) as e:
        logger.error(f"[TRANSFORM] Validation failed: {e}")
        return None


def _classify_heat(temp_c: float) -> str:
    if temp_c < 10:   return "Cold"
    elif temp_c < 20: return "Cool"
    elif temp_c < 30: return "Comfortable"
    elif temp_c < 38: return "Hot"
    else:             return "Extreme Heat"


def _classify_wind(speed_mps: float) -> str:
    if speed_mps < 1:    return "Calm"
    elif speed_mps < 5:  return "Light Breeze"
    elif speed_mps < 10: return "Moderate Wind"
    elif speed_mps < 20: return "Strong Wind"
    else:                return "Storm"


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 3 — LOAD  (SQLite as Data Warehouse)
# ══════════════════════════════════════════════════════════════════════════════

def init_warehouse(db_path: str = DB_PATH):
    """
    Create the data warehouse schema if it doesn't exist.
    Uses a star-schema-style design: fact table + dimension hint fields.
    """
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_weather (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            city             TEXT    NOT NULL,
            country          TEXT,
            ingested_at      TEXT    NOT NULL,
            data_source      TEXT,
            temp_celsius     REAL,
            feels_like_c     REAL,
            temp_min_c       REAL,
            temp_max_c       REAL,
            temp_fahrenheit  REAL,
            humidity_pct     INTEGER,
            pressure_hpa     INTEGER,
            wind_speed_mps   REAL,
            wind_deg         INTEGER,
            wind_label       TEXT,
            visibility_m     INTEGER,
            cloud_cover_pct  INTEGER,
            condition        TEXT,
            condition_desc   TEXT,
            heat_category    TEXT,
            raw_payload      TEXT
        )
    """)
    conn.commit()
    conn.close()
    logger.info("[LOAD] Warehouse schema initialized.")


def load(record: dict, db_path: str = DB_PATH):
    """Load a transformed record into the SQLite data warehouse."""
    logger.info(f"[LOAD] Writing record for {record['city']} to warehouse...")
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO fact_weather (
            city, country, ingested_at, data_source,
            temp_celsius, feels_like_c, temp_min_c, temp_max_c, temp_fahrenheit,
            humidity_pct, pressure_hpa,
            wind_speed_mps, wind_deg, wind_label,
            visibility_m, cloud_cover_pct,
            condition, condition_desc, heat_category, raw_payload
        ) VALUES (
            :city, :country, :ingested_at, :data_source,
            :temp_celsius, :feels_like_c, :temp_min_c, :temp_max_c, :temp_fahrenheit,
            :humidity_pct, :pressure_hpa,
            :wind_speed_mps, :wind_deg, :wind_label,
            :visibility_m, :cloud_cover_pct,
            :condition, :condition_desc, :heat_category, :raw_payload
        )
    """, record)
    conn.commit()
    conn.close()
    logger.info(f"[LOAD] ✓ Inserted — {record['city']} @ {record['ingested_at']}")


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 4 — ANALYTICS  (SQL-based aggregations)
# ══════════════════════════════════════════════════════════════════════════════

def run_analytics(db_path: str = DB_PATH):
    """Run analytical queries on the warehouse — mimics a reporting/BI layer."""
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    print("\n" + "═"*60)
    print("  📊  WEATHER DATA WAREHOUSE — ANALYTICS REPORT")
    print("═"*60)

    # KPI 1: Latest reading per city
    print("\n📍 Latest Temperature by City:")
    cur.execute("""
        SELECT city, temp_celsius, condition_desc, heat_category, ingested_at
        FROM fact_weather
        WHERE ingested_at = (
            SELECT MAX(ingested_at) FROM fact_weather f2
            WHERE f2.city = fact_weather.city
        )
        ORDER BY temp_celsius DESC
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:<15} {row[1]:>6}°C  |  {row[2]:<25} [{row[3]}]  @  {row[4]}")

    # KPI 2: Average metrics per city (historical)
    print("\n📈 Historical Averages per City:")
    cur.execute("""
        SELECT city,
               ROUND(AVG(temp_celsius),2)   AS avg_temp,
               ROUND(AVG(humidity_pct),1)   AS avg_humidity,
               ROUND(AVG(wind_speed_mps),2) AS avg_wind,
               COUNT(*)                     AS total_records
        FROM fact_weather
        GROUP BY city
        ORDER BY avg_temp DESC
    """)
    print(f"   {'City':<15} {'Avg Temp':>10} {'Avg Hum%':>10} {'Avg Wind':>10} {'Records':>10}")
    print("   " + "-"*55)
    for row in cur.fetchall():
        print(f"   {row[0]:<15} {row[1]:>9}°C {row[2]:>9}%  {row[3]:>8} m/s {row[4]:>8}")

    # KPI 3: Condition frequency
    print("\n☁  Condition Distribution (all runs):")
    cur.execute("""
        SELECT condition, COUNT(*) as freq
        FROM fact_weather
        GROUP BY condition
        ORDER BY freq DESC
    """)
    for row in cur.fetchall():
        print(f"   {row[0]:<20}  {row[1]} records")

    print("\n" + "═"*60 + "\n")
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATOR  — ties all layers together
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(cities: list = CITIES):
    """
    Orchestrates the full ETL pipeline:
      Extract → Transform → Load → Analyze
    """
    logger.info("=" * 60)
    logger.info("  WEATHER ETL PIPELINE STARTED")
    logger.info("=" * 60)

    init_warehouse()

    success, failed = 0, 0

    for city in cities:
        logger.info(f"\n{'─'*40}\n Processing: {city}\n{'─'*40}")

        # E → T → L
        raw    = extract(city)
        if not raw:
            failed += 1
            continue

        record = transform(raw)
        if not record:
            failed += 1
            continue

        load(record)
        success += 1

    logger.info(f"\n[PIPELINE] Completed — ✓ {success} succeeded | ✗ {failed} failed")

    # Analytics layer
    run_analytics()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_pipeline()
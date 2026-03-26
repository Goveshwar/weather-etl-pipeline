# 🌦️ Weather ETL Data Pipeline

### A production-style Data Engineering portfolio project

![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-Data%20Warehouse-003B57?style=for-the-badge&logo=sqlite&logoColor=white)
![REST API](https://img.shields.io/badge/REST-API%20Ingestion-FF6C37?style=for-the-badge&logo=postman&logoColor=white)
![ETL](https://img.shields.io/badge/ETL-Pipeline-00C853?style=for-the-badge)

> Extracts real-time weather data from the OpenWeatherMap API · Transforms & validates it · Loads into a SQLite data warehouse · Runs SQL-based analytics

</div>

---

## 📌 Project Overview

This project simulates a **real-world Data Engineering workflow** — from raw API data to a queryable data warehouse. It was built to demonstrate core DE skills including API ingestion, ETL design, data quality checks, schema design, and analytical reporting.

| 🔹 Role Target | 🔹 Level | 🔹 Domain |
|---|---|---|
| Data Engineer | Fresher / Entry-level | Weather & Climate Data |

---

## 🏗️ Pipeline Architecture

```
┌─────────────────────────────────────────────────────┐
│              OpenWeatherMap REST API                 │
│         (Real-time weather for 5 Indian cities)      │
└─────────────────────┬───────────────────────────────┘
                      │  Raw JSON
                      ▼
           ┌──────────────────┐
           │   EXTRACT Layer  │  HTTP GET · Timeout handling
           │                  │  HTTP error codes · Retry logic
           └────────┬─────────┘
                    │  Validated raw payload
                    ▼
           ┌──────────────────┐
           │ TRANSFORM Layer  │  Data validation · Type casting
           │                  │  Feature engineering · Unit conversion
           │                  │  Business rule classification
           └────────┬─────────┘
                    │  Clean structured record
                    ▼
           ┌──────────────────┐
           │   LOAD Layer     │  SQLite Data Warehouse
           │                  │  fact_weather table
           │                  │  Star-schema design
           └────────┬─────────┘
                    │
                    ▼
           ┌──────────────────┐
           │ ANALYTICS Layer  │  SQL aggregations · KPI reporting
           │                  │  City comparisons · Trend analysis
           └──────────────────┘
```

---

## ✨ Data Engineering Concepts Demonstrated

| Concept | How It's Applied |
|---|---|
| **ETL Pipeline** | Clean separation of Extract, Transform, Load layers |
| **API Ingestion** | `requests` library with timeout, status codes & exception handling |
| **Data Validation** | Range checks, type assertions, null handling |
| **Feature Engineering** | Derived fields — heat category, wind label, °F conversion |
| **Data Warehouse Design** | SQLite `fact_weather` table with star-schema principles |
| **Raw Data Preservation** | Full JSON payload stored for reprocessing / auditing |
| **SQL Analytics** | Aggregations, GROUP BY, subqueries, window-style latest-per-city |
| **Structured Logging** | Logs written to both console and `pipeline.log` file |
| **Orchestration** | Single `run_pipeline()` orchestrator ties all layers together |
| **Idempotency** | Append-only design — every run builds historical data |

---

## 🗂️ Project Structure

```
weather-etl-pipeline/
│
├── weatherapp.py          # Main ETL pipeline (Extract → Transform → Load → Analyze)
├── requirements.txt       # Python dependencies
├── .gitignore             # Files excluded from Git
└── README.md              # You are here 📍
```

> **Generated at runtime** (not uploaded to GitHub):
> - `weather_warehouse.db` — SQLite data warehouse
> - `pipeline.log` — Execution logs

---

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- Free API key from [OpenWeatherMap](https://openweathermap.org/api)

### Step 1 — Clone the repository
```bash
git clone https://github.com/your-username/weather-etl-pipeline.git
cd weather-etl-pipeline
```

### Step 2 — Install dependencies
```bash
pip install -r requirements.txt
```

### Step 3 — Add your API key
Open `weatherapp.py` and replace in the config section:
```python
API_KEY = "your_openweathermap_api_key_here"
```

### Step 4 — Run the pipeline
```bash
python weatherapp.py
```

---

## 📊 Sample Analytics Output

```
════════════════════════════════════════════════════════════
  📊  WEATHER DATA WAREHOUSE — ANALYTICS REPORT
════════════════════════════════════════════════════════════

📍 Latest Temperature by City:
   Pune            32.5°C  |  Partly Cloudy           [Hot]        @ 2024-06-01 10:30:00
   Mumbai          31.0°C  |  Haze                    [Hot]        @ 2024-06-01 10:30:00
   Hyderabad       35.2°C  |  Clear Sky               [Hot]        @ 2024-06-01 10:30:00

📈 Historical Averages per City:
   City            Avg Temp   Avg Hum%   Avg Wind    Records
   -------------------------------------------------------
   Hyderabad         34.10°C      45.0%      3.20 m/s        5
   Delhi             33.80°C      38.0%      4.10 m/s        5
   Pune              31.50°C      55.0%      2.80 m/s        5
```

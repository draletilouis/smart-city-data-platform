import requests
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine
import json

# ----------------------------
# CONFIG
# ----------------------------
RAW_DIR = "data/raw/api/"
STAGING_DIR = "data/staging/api/"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

WAREHOUSE_URI = "postgresql://admin:admin@localhost:5432/smartcity_warehouse"

# Multiple cities (name, latitude, longitude)
CITIES = [
    {"name": "Kampala", "lat": 0.3476, "lon": 32.5825},
    {"name": "Entebbe", "lat": 0.0500, "lon": 32.4600},
    {"name": "Gulu", "lat": 2.7667, "lon": 32.3056},
    {"name": "Mbarara", "lat": -0.6072, "lon": 30.6545},
    {"name": "Fort Portal", "lat": 0.6710, "lon": 30.2750}
]


# ----------------------------
# EXTRACTION
# ----------------------------
def extract_weather_data():
    all_data = []

    for city in CITIES:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "current_weather": "true"
        }

        response = requests.get(url, params=params)
        data = response.json()
        data["city"] = city["name"]
        data["extracted_at"] = datetime.now().isoformat()
        all_data.append(data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(RAW_DIR, f"weather_raw_batch_{timestamp}.json")

    with open(file_path, "w") as f:
        json.dump(all_data, f, indent=4)

    print(f" Extracted weather data for {len(CITIES)} cities → {file_path}")
    return file_path


# ----------------------------
# CLEAN & STAGE
# ----------------------------
def clean_and_stage(raw_path):
    with open(raw_path, "r") as f:
        data = json.load(f)

    # Flatten data from multiple cities
    records = []
    for entry in data:
        weather = entry.get("current_weather", {})
        record = {
            "city": entry["city"],
            "temperature": weather.get("temperature"),
            "windspeed": weather.get("windspeed"),
            "winddirection": weather.get("winddirection"),
            "weathercode": weather.get("weathercode"),
            "time": weather.get("time"),
            "extracted_at": entry["extracted_at"]
        }
        records.append(record)

    df = pd.DataFrame(records)
    staged_file = os.path.join(STAGING_DIR, "weather_staged.csv")
    df.to_csv(staged_file, index=False)
    print(f"💾 Cleaned and staged data for {len(df)} records → {staged_file}")
    return staged_file


# ----------------------------
# LOAD
# ----------------------------
def load_to_warehouse(staged_file):
    df = pd.read_csv(staged_file)
    engine = create_engine(WAREHOUSE_URI)
    df.to_sql("weather_data", engine, if_exists="append", index=False)
    print(f"Loaded {len(df)} records into warehouse from {staged_file}")


# ----------------------------
# MASTER RUNNER
# ----------------------------
def run_weather_pipeline():
    raw_path = extract_weather_data()
    staged_file = clean_and_stage(raw_path)
    load_to_warehouse(staged_file)
    print("Multi-city Weather API ingestion complete!")


if __name__ == "__main__":
    run_weather_pipeline()

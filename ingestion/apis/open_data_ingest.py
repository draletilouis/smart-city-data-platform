import requests
import pandas as pd
import os
import json
from datetime import datetime
from sqlalchemy import create_engine


# CONFIG

RAW_DIR = "data/raw/openaq/"
STAGING_DIR = "data/staging/openaq/"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

# Use environment variables for Docker, fallback to localhost for local development
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "smartcity_warehouse")
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")

WAREHOUSE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Cities of interest
CITIES = ["Kampala", "Entebbe", "Gulu", "Mbarara", "Fort Portal"]

# Base API URL
BASE_URL = "https://api.openaq.org/v3/latest"

# API Key (optional - some endpoints require authentication)
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")




# EXTRACT

def extract_air_quality_data():
    all_data = []

    # Prepare headers with API key if available
    headers = {}
    if OPENAQ_API_KEY:
        headers["X-API-Key"] = OPENAQ_API_KEY

    for city in CITIES:
        params = {
            "city": city,
            "limit": 100
        }
        try:
            response = requests.get(BASE_URL, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                data["city"] = city
                data["extracted_at"] = datetime.now().isoformat()
                all_data.append(data)
                print(f"Successfully fetched data for {city}")
            else:
                print(f"Failed to fetch data for {city}: {response.status_code}")
                if response.status_code == 401:
                    print("Note: OpenAQ API requires an API key. Get one at https://openaq.org/")
        except Exception as e:
            print(f"Error fetching data for {city}: {str(e)}")

    if not all_data:
        print("WARNING: No data was fetched from OpenAQ API")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_path = os.path.join(RAW_DIR, f"openaq_raw_{timestamp}.json")

    with open(raw_path, "w") as f:
        json.dump(all_data, f, indent=4)

    print(f"Extracted air quality data for {len(CITIES)} cities → {raw_path}")
    return raw_path


# CLEAN & STAGE
 
def clean_and_stage(raw_path):
    with open(raw_path, "r") as f:
        data = json.load(f)

    records = []
    for entry in data:
        for result in entry.get("results", []):
            city = result.get("city")
            location = result.get("location")
            coords = result.get("coordinates", {})
            latitude = coords.get("latitude")
            longitude = coords.get("longitude")

            for m in result.get("measurements", []):
                records.append({
                    "city": city,
                    "location": location,
                    "parameter": m.get("parameter"),
                    "value": m.get("value"),
                    "unit": m.get("unit"),
                    "last_updated": m.get("lastUpdated"),
                    "latitude": latitude,
                    "longitude": longitude,
                    "extracted_at": entry["extracted_at"]
                })

    df = pd.DataFrame(records)
    df.drop_duplicates(inplace=True)
    staged_path = os.path.join(STAGING_DIR, "air_quality_staged.csv")
    df.to_csv(staged_path, index=False)
    print(f"Cleaned and staged {len(df)} air quality records → {staged_path}")
    return staged_path


# LOAD

def load_to_warehouse(staged_path):
    df = pd.read_csv(staged_path)

    if df.empty:
        print("No data to load into warehouse (empty dataframe)")
        return

    engine = create_engine(WAREHOUSE_URI)
    df.to_sql("air_quality_data", engine, if_exists="append", index=False)
    print(f"Loaded {len(df)} records into warehouse table 'air_quality_data'")

# MASTER RUNNER

def run_openaq_pipeline():
    raw = extract_air_quality_data()

    if raw is None:
        print("ERROR: No data extracted. Please check your OpenAQ API key.")
        print("To get an API key, visit: https://openaq.org/")
        print("Then add it to your .env file as: OPENAQ_API_KEY=your_key_here")
        return

    staged = clean_and_stage(raw)
    load_to_warehouse(staged)
    print("OpenAQ (Air Quality) ingestion complete.")


if __name__ == "__main__":
    run_openaq_pipeline()

import requests
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm

API_KEY = "me-nhi-batunga"
HEADERS = {"X-API-Key": API_KEY}

MAJOR_CITIES = ["Delhi", "Mumbai", "Kolkata", "Chennai", "Bangalore"]
START_DATE = "2025-01-18"
END_DATE = "2025-06-18"
PARAMS = ["pm25", "pm10", "rh", "temp", "ws"]

BASE_URL = "https://api.openaq.org/v3"

def get_city_location_ids(city_name):
    """Get location IDs for a city."""
    location_ids = []
    page = 1
    while True:
        response = requests.get(f"{BASE_URL}/locations", headers=HEADERS, params={
            "city": city_name,
            "limit": 100,
            "page": page
        })
        if response.status_code != 200:
            print(f"❌ Failed to fetch locations for {city_name}: {response.status_code}")
            break

        data = response.json().get("results", [])
        if not data:
            break

        location_ids.extend([loc["id"] for loc in data])
        if len(data) < 100:
            break
        page += 1
        time.sleep(0.2)
    return location_ids

def fetch_measurements(location_id, parameter):
    """Fetch daily measurements for a given location and parameter."""
    results = []
    page = 1
    while True:
        response = requests.get(f"{BASE_URL}/measurements", headers=HEADERS, params={
            "location_id": location_id,
            "parameter": parameter,
            "date_from": START_DATE,
            "date_to": END_DATE,
            "limit": 100,
            "page": page,
            "aggregate": "day"
        })
        if response.status_code == 404:
            break
        elif response.status_code != 200:
            print(f"❌ Error {response.status_code} for param {parameter} at location {location_id}")
            break
        data = response.json().get("results", [])
        if not data:
            break
        results.extend(data)
        if len(data) < 100:
            break
        page += 1
        time.sleep(0.2)
    return results

def main():
    records = []

    for city in MAJOR_CITIES:
        print(f"🌆 Processing city: {city}")
        loc_ids = get_city_location_ids(city)
        print(f"🧭 Found {len(loc_ids)} locations for {city}")

        for loc_id in tqdm(loc_ids, desc=f"📍 {city}"):
            for param in PARAMS:
                try:
                    data = fetch_measurements(loc_id, param)
                    for r in data:
                        records.append({
                            "city": city,
                            "location_id": loc_id,
                            "location_name": r.get("location"),
                            "latitude": r.get("coordinates", {}).get("latitude"),
                            "longitude": r.get("coordinates", {}).get("longitude"),
                            "date": r["date"]["utc"][:10],
                            "parameter": r["parameter"],
                            "value": r["value"],
                            "unit": r["unit"]
                        })
                except Exception as e:
                    print(f"⚠️ Error for {city} loc {loc_id} param {param}: {e}")
                time.sleep(0.1)

    df = pd.DataFrame(records)
    if df.empty:
        print("❌ No data collected.")
        return

    print("📊 Structuring data...")
    df_pivot = df.pivot_table(
        index=["city", "location_id", "location_name", "latitude", "longitude", "date"],
        columns="parameter", values="value"
    ).reset_index()

    df_pivot.to_csv("major_cities_air_quality.csv", index=False)
    print("✅ Saved to major_cities_air_quality.csv")

main()


import requests
import pandas as pd
import os
import time
from datetime import datetime

# Configuration
API_KEY = "573449fef84af782b345ec8aadd217e24d20dafd65f6865d4e83ab6871f4fd33"  # Set this if required, else keep None
OUTPUT_DIR = "openaq_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Rate limiting
REQUEST_DELAY = 1  # seconds between requests

# Important parameters for PM prediction
IMPORTANT_PARAMETERS = {
    'pm25': 'PM2.5',
    'pm10': 'PM10', 
    'no2': 'NO2',
    'so2': 'SO2',
    'o3': 'Ozone',
    'temperature': 'Temperature',
    'relativehumidity': 'Relative Humidity',
    'pressure': 'Atmospheric Pressure',
    'ws': 'Wind Speed',
    'wd': 'Wind Direction'
}

# Date range - let's use a shorter, more recent range to test
date_from = "2025-01-01T00:00:00Z"
date_to = "2025-01-31T23:59:59Z"
limit = 1000  # Reduced limit to avoid overwhelming the API

# Headers
headers = {"x-api-key": API_KEY} if API_KEY else {}

# Location IDs to fetch data for
location_ids = [301387, 11579, 6981, 234568, 278671]

def filter_important_sensors(sensors):
    """Filter sensors to only include those with important parameters for PM prediction"""
    important_sensors = []
    
    for sensor in sensors:
        parameter_info = sensor.get('parameter', {})
        parameter_name = parameter_info.get('name', '').lower()
        
        # Check if this parameter is in our important list
        if parameter_name in IMPORTANT_PARAMETERS:
            important_sensors.append(sensor)
    
    return important_sensors

def get_sensors_for_location(location_id):
    """Get all sensors for a specific location"""
    url = f"https://api.openaq.org/v3/locations/{location_id}/sensors"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        time.sleep(REQUEST_DELAY)  # Rate limiting
        return data.get("results", [])
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 422:
            print(f"❌ Location {location_id} not found or invalid")
        else:
            print(f"❌ HTTP Error fetching sensors for location {location_id}: {e}")
        return []
    except Exception as e:
        print(f"❌ Error fetching sensors for location {location_id}: {e}")
        return []

def fetch_measurements_for_sensor(sensor_id):
    """Fetch hourly aggregated measurements for a specific sensor using the correct endpoint"""
    page = 1
    all_results = []
    
    while True:
        # Use the correct endpoint for hourly aggregated measurements
        url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements/hourly"
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "page": page,
        }
        
        try:
            time.sleep(REQUEST_DELAY)  # Rate limiting
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            results = data.get("results", [])
            
            # Debug: Show structure of first result on first page
            if page == 1 and results:
                print(f"  🔍 Debug: Sample API response structure:")
                sample_result = results[0]
                print(f"       Keys: {list(sample_result.keys())}")
                if len(str(sample_result)) < 300:
                    print(f"       Sample: {sample_result}")
                else:
                    print(f"       Sample: {str(sample_result)[:300]}...")
            if not results:
                break
                
            all_results.extend(results)
            print(f"  📊 Page {page}: Got {len(results)} hourly records")
            
            # Simple pagination check - if we get fewer results than limit, we're done
            if len(results) < limit:
                break
                
            page += 1
            
            # Safety check to avoid infinite loops
            if page > 100:  # Reasonable limit for hourly data
                print(f"  ⚠️  Reached page limit, stopping")
                break
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  ⚠️  No hourly data available for sensor {sensor_id} in date range")
            elif e.response.status_code == 429:
                print(f"  ⏳ Rate limited, waiting 5 seconds...")
                time.sleep(5)
                continue  # Retry the same request
            else:
                print(f"  ❌ HTTP Error {e.response.status_code} for sensor {sensor_id}")
            break
        except Exception as e:
            print(f"  ❌ Error fetching hourly data for sensor {sensor_id}: {e}")
            break
    
    return all_results

def clean_measurement_data(measurements):
    """Clean and standardize measurement data"""
    cleaned_data = []
    
    # Debug: Show structure of first measurement
    if measurements:
        print(f"  🔍 Debug: First measurement structure:")
        first_measurement = measurements[0]
        print(f"       Keys: {list(first_measurement.keys())}")
        print(f"       Sample: {str(first_measurement)[:200]}...")
    
    for measurement in measurements:
        try:
            # Extract value
            value = measurement.get('value')
            
            # Extract datetime from the nested period structure
            datetime_utc = None
            period = measurement.get('period', {})
            
            if period:
                # Try datetimeFrom first, then datetimeTo
                datetime_from = period.get('datetimeFrom', {})
                datetime_to = period.get('datetimeTo', {})
                
                if datetime_from and 'utc' in datetime_from:
                    datetime_utc = datetime_from['utc']
                elif datetime_to and 'utc' in datetime_to:
                    datetime_utc = datetime_to['utc']
            
            # Fallback to other possible datetime fields
            if not datetime_utc:
                datetime_utc = (measurement.get('datetime') or 
                              measurement.get('date') or 
                              measurement.get('utc') or
                              measurement.get('timestamp'))
            
            # Skip invalid measurements
            if value is None or datetime_utc is None:
                continue
                
            # Skip obviously invalid values (negative concentrations, extreme values)
            # Be more lenient with thresholds - some parameters can have higher values
            if value < 0 or value > 50000:  # Increased threshold
                continue
            
            # Extract parameter info
            parameter_info = measurement.get('parameter', {})
            if isinstance(parameter_info, dict):
                parameter_name = parameter_info.get('name', 'unknown')
                parameter_unit = parameter_info.get('units', 'unknown')
            else:
                parameter_name = measurement.get('parameter', 'unknown')
                parameter_unit = 'unknown'
            
            cleaned_record = {
                'datetime_utc': datetime_utc,
                'value': value,
                'parameter': parameter_name,
                'unit': parameter_unit,
                'sensor_id': measurement.get('sensor_id'),
                'location_id': measurement.get('location_id'),
                'coordinates': measurement.get('coordinates', {}),
                'summary': measurement.get('summary', {}),
                'period_label': period.get('label', 'unknown') if period else 'unknown'
            }
            
            cleaned_data.append(cleaned_record)
            
        except Exception as e:
            print(f"  ⚠️  Error cleaning measurement: {e}")
            continue
    
    print(f"  📊 Cleaning result: {len(cleaned_data)} valid records out of {len(measurements)} total")
    return cleaned_data

def fetch_hourly_data_for_sensor(sensor_id):
    """Fetch hourly averaged data for a specific sensor (alternative method name for compatibility)"""
    return fetch_measurements_for_sensor(sensor_id)

def process_all_locations():
    """Process all locations and fetch their sensor measurements"""
    for loc_id in location_ids:
        print(f"🔍 Processing location {loc_id}...")
        
        # Get all sensors for this location
        sensors = get_sensors_for_location(loc_id)
        
        if not sensors:
            print(f"⚠️  No sensors found for location {loc_id}")
            continue
        
        # Filter to only important parameters
        important_sensors = filter_important_sensors(sensors)
        
        if not important_sensors:
            print(f"⚠️  No important sensors found for location {loc_id}")
            continue
            
        print(f"📡 Found {len(important_sensors)} important sensors (out of {len(sensors)} total)")
        
        # Show which parameters we found
        found_params = [s.get('parameter', {}).get('name', 'unknown') for s in important_sensors]
        print(f"🎯 Parameters: {', '.join(found_params)}")
        
        all_location_data = []
        
        # Fetch measurements for each important sensor
        for i, sensor in enumerate(important_sensors, 1):
            sensor_id = sensor.get('id')
            parameter_info = sensor.get('parameter', {})
            parameter_name = parameter_info.get('name', 'unknown')
            
            print(f"📥 [{i}/{len(important_sensors)}] Fetching sensor {sensor_id} (parameter: {parameter_name})")
            
            measurements = fetch_measurements_for_sensor(sensor_id)
            
            if measurements:
                # Add sensor info to each measurement for context
                for measurement in measurements:
                    measurement['sensor_id'] = sensor_id
                    measurement['location_id'] = loc_id
                    measurement['parameter'] = parameter_name
                    
                all_location_data.extend(measurements)
                print(f"✅ Got {len(measurements)} total measurements for sensor {sensor_id}")
            else:
                print(f"⚠️  No measurements for sensor {sensor_id}")
        
        # Clean and save data for this location
        if all_location_data:
            # Clean the data
            cleaned_data = clean_measurement_data(all_location_data)
            
            if cleaned_data:
                df = pd.DataFrame(cleaned_data)
                
                # Sort by datetime for better analysis
                df['datetime_utc'] = pd.to_datetime(df['datetime_utc'])
                df = df.sort_values('datetime_utc')
                
                # Save cleaned data
                filename = f"location_{loc_id}_cleaned.csv"
                filepath = os.path.join(OUTPUT_DIR, filename)
                df.to_csv(filepath, index=False)
                
                # Show summary of what we got
                param_counts = df['parameter'].value_counts()
                print(f"✅ Saved {len(df)} cleaned records to {filename}")
                print("📊 Parameter breakdown:")
                for param, count in param_counts.items():
                    print(f"   - {param}: {count} measurements")
            else:
                print(f"⚠️  No valid data after cleaning for location {loc_id}")
        else:
            print(f"⚠️  No data found for location {loc_id}")
        
        print(f"{'='*50}")

if __name__ == "__main__":
    print("🚀 Starting OpenAQ data fetch for PM prediction...")
    print(f"📅 Date range: {date_from} to {date_to}")
    print(f"📍 Processing {len(location_ids)} locations")
    print(f"⏱️  Request delay: {REQUEST_DELAY}s")
    print(f"🎯 Target parameters: {', '.join(IMPORTANT_PARAMETERS.values())}")
    process_all_locations()
    print("🎉 Data fetch complete!")
    print("\n💡 Next steps for PM prediction:")
    print("   1. Load the cleaned CSV files")
    print("   2. Merge data by datetime and location")
    print("   3. Handle missing values and outliers") 
    print("   4. Create features like hour, day_of_week, season")
    print("   5. Build prediction models (Random Forest, XGBoost, etc.)")
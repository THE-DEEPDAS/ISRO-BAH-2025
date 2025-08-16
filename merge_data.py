import pandas as pd
import xarray as xr
import h5py
import numpy as np
from pathlib import Path

# ======================
# USER CONFIG
# ======================
CPCB_FOLDER = Path("data/cpcb/")             # CPCB daily CSV folder
MERRA_FILE = Path("data/merra2.nc4")         # MERRA-2 hourly nc4
MOSDAC_FOLDER = Path("data/mosdac_aod/")     # Folder with MOSDAC h5 files

SITE_COORDS = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Kolkata": (22.5726, 88.3639),
    "Chennai": (13.0827, 80.2707),
    "Bengaluru": (12.9716, 77.5946),
}

# ======================
# 1. Load CPCB Daily CSVs
# ======================
def load_cpcb(folder):
    all_dfs = []
    for csv_file in folder.glob("*.csv"):
        site_name = csv_file.stem
        df = pd.read_csv(csv_file)
        # Parse datetime columns and create a 'date' column (date only)
        if "period.datetimeFrom.local" in df.columns:
            df["date"] = pd.to_datetime(df["period.datetimeFrom.local"]).dt.date
        elif "period.datetimeTo.local" in df.columns:
            df["date"] = pd.to_datetime(df["period.datetimeTo.local"]).dt.date
        else:
            raise ValueError("No datetime column found in CPCB file: " + str(csv_file))
        df["site"] = site_name
        all_dfs.append(df)
    return pd.concat(all_dfs, ignore_index=True)

cpcb_df = load_cpcb(CPCB_FOLDER)

# ======================
# 2. Extract daily MERRA-2 for each site
# ======================
def extract_daily_from_nc4(nc4_file, var_names, site_coords):
    ds = xr.open_dataset(nc4_file)
    all_sites = []
    for site, (lat, lon) in site_coords.items():
        site_ds = ds.sel(lat=lat, lon=lon, method="nearest")
        site_df = site_ds[var_names].to_dataframe().reset_index()
        site_df = site_df.resample("1D", on="time").mean().reset_index()
        site_df["site"] = site
        site_df.rename(columns={"time": "date"}, inplace=True)
        all_sites.append(site_df)
    return pd.concat(all_sites, ignore_index=True)

merra_vars = ["T2M", "RH2M", "U2M", "V2M", "PS"]  # Change to your MERRA vars
merra_df = extract_daily_from_nc4(MERRA_FILE, merra_vars, SITE_COORDS)

# ======================
# 3. Extract daily AOD from MOSDAC (.h5)
# ======================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))

def extract_daily_from_h5(folder, var_name, site_coords):
    all_data = []
    for h5_file in sorted(folder.glob("*.h5")):
        with h5py.File(h5_file, "r") as f:
            # Adjust dataset paths according to MOSDAC file structure
            lat = f["Latitude"][:]
            lon = f["Longitude"][:]
            aod = f[var_name][:]  # e.g., "AOD"

            # Extract timestamp from filename (adjust as per your naming convention)
            date_str = h5_file.stem.split("_")[1]  # example: INSAT3DR_20230101_xxx
            timestamp = pd.to_datetime(date_str, format="%Y%m%d")

            for site, (s_lat, s_lon) in site_coords.items():
                dist = haversine(s_lat, s_lon, lat, lon)
                min_idx = np.unravel_index(dist.argmin(), dist.shape)
                site_aod = aod[min_idx]
                all_data.append({"site": site, "date": timestamp, "AOD": site_aod})

    df = pd.DataFrame(all_data)
    daily_df = df.groupby(["site", "date"], as_index=False).mean()
    return daily_df

aod_df = extract_daily_from_h5(MOSDAC_FOLDER, "AOD", SITE_COORDS)

# ======================
# 4. Merge all datasets
# ======================
merged = cpcb_df.merge(merra_df, on=["site", "date"], how="inner")
merged = merged.merge(aod_df, on=["site", "date"], how="inner")

# ======================
# 5. Save
# ======================
merged.to_csv("merged_daily_pm_dataset.csv", index=False)
print("✅ Merged dataset saved:", merged.shape)

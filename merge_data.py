import pandas as pd
import xarray as xr
import numpy as np
from pathlib import Path

# ======================
# USER CONFIG
# ======================
CPCB_FOLDER = Path("CPCB")             # CPCB daily CSV folder
MERRA_FOLDER = Path("Merra-2")         # MERRA-2 hourly nc4
MOSDAC_FOLDER = Path("mosdac")     # Folder with MOSDAC h5 files

# Coordinates provided by user, will be mapped to CPCB site names by filename order
COORD_LIST = [
    (13.1278, 80.2642),
    (26.428282, 80.327067),
    (25.4547, 78.6039),
    (23.020509, 72.579261),
    (22.55664, 88.342674),
]

# ======================
# 1. Load CPCB Daily CSVs
# ======================
def load_cpcb(folder):
    all_dfs = []
    for csv_file in sorted(folder.glob("*.csv")):
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

# Build SITE_COORDS mapping using CPCB file stems in sorted order
_cpcb_sites = sorted([p.stem for p in CPCB_FOLDER.glob("*.csv")])
if len(_cpcb_sites) != len(COORD_LIST):
    raise ValueError(
        f"Expected {len(COORD_LIST)} CPCB sites to map to coordinates, found {_cpcb_sites}"
    )
SITE_COORDS = {name: coord for name, coord in zip(_cpcb_sites, COORD_LIST)}

# ======================
# 2. Extract daily MERRA-2 for each site
# ======================
def extract_daily_from_merra_folder(folder, var_names, site_coords):
    """Open all .nc4 MERRA files in `folder`, extract the requested variables
    at the nearest grid point for each site and compute daily means.

    Returns a single DataFrame with columns: ['date', <vars...>, 'site']
    """
    files = sorted(folder.glob("*.nc4"))
    if not files:
        raise ValueError(f"No .nc4 files found in MERRA folder: {folder}")

    # Use xarray to open multiple files as a single dataset (by coordinates).
    # Try netcdf4 first, then h5netcdf if unavailable.
    paths = [str(p) for p in files]
    ds = None
    last_err = None
    for eng in ("netcdf4", "h5netcdf"):
        try:
            ds = xr.open_mfdataset(paths, combine="by_coords", engine=eng, chunks={"time": 24})
            break
        except Exception as e:
            last_err = e
            ds = None
    if ds is None:
        raise RuntimeError(
            "Failed to open MERRA files. Tried engines: netcdf4, h5netcdf. "
            "Please install one of them (pip install netCDF4 h5netcdf cftime).\n"
            f"Last error: {last_err}"
        )

    # Determine which requested variables exist in the dataset
    avail_vars = [v for v in var_names if v in ds.data_vars]
    missing_vars = [v for v in var_names if v not in ds.data_vars]
    if missing_vars:
        print(f"⚠️ Missing MERRA variables (will skip): {missing_vars}")
    if not avail_vars:
        raise RuntimeError(
            "None of the requested MERRA variables are present. Available variables include: "
            + ", ".join(list(ds.data_vars.keys())[:30])
        )

    all_sites = []
    for site, (lat, lon) in site_coords.items():
        sel_point = ds.sel(lat=lat, lon=lon, method="nearest")
        sel = sel_point[avail_vars]
        df = sel.to_dataframe().reset_index()
        # ensure time is datetime and resample to daily means
        df["time"] = pd.to_datetime(df["time"]) if "time" in df.columns else pd.to_datetime(df.index)
        daily = df.set_index("time").resample("1D").mean().reset_index()
        daily["site"] = site
        daily.rename(columns={"time": "date"}, inplace=True)
        # unify to date (no time) to match CPCB
        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        all_sites.append(daily)

    if not all_sites:
        raise RuntimeError("No MERRA daily data could be computed for the requested sites/variables.")
    return pd.concat(all_sites, ignore_index=True)

merra_vars = ["T2M", "RH2M", "U2M", "V2M", "PS"]  # Change to your MERRA vars
merra_df = extract_daily_from_merra_folder(MERRA_FOLDER, merra_vars, SITE_COORDS)

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
    # Lazy import h5py (required); raise if unavailable since AOD merge is compulsory
    try:
        import h5py  # type: ignore
    except Exception as e:
        raise RuntimeError(
            f"h5py import failed ({e}). AOD (MOSDAC) merge is required. "
            "Please install matching h5py and HDF5 in your environment (e.g., conda install -c conda-forge h5py hdf5)."
        )

    files = sorted(folder.glob("*.h5"))
    if not files:
        raise FileNotFoundError(f"No MOSDAC .h5 files found in {folder}. AOD is required.")

    def _find_dataset(h5obj, name_hints=None, contains=None):
        """Search recursively for a dataset path that matches hints.
        Returns the first matching path or None.
        """
        if name_hints is None:
            name_hints = []
        if contains is None:
            contains = []
        stack = [("/", h5obj)]
        while stack:
            path, obj = stack.pop()
            if hasattr(obj, "keys"):
                for k in obj.keys():
                    child = obj[k]
                    child_path = path.rstrip("/") + "/" + k
                    # dataset
                    if getattr(child, "shape", None) is not None and hasattr(child, "dtype"):
                        key_lower = k.lower()
                        if any(h.lower() == k.lower() for h in name_hints) or any(c in key_lower for c in contains):
                            return child_path
                    else:
                        stack.append((child_path, child))
        return None

    all_data = []
    for h5_file in files:
        with h5py.File(h5_file, "r") as f:
            # Try multiple possible paths
            lat_path = _find_dataset(f, name_hints=["Latitude"], contains=["lat"])
            lon_path = _find_dataset(f, name_hints=["Longitude"], contains=["lon"])
            aod_path = _find_dataset(f, name_hints=[var_name, "AOD"], contains=["aod"])
            if not (lat_path and lon_path and aod_path):
                raise KeyError(
                    f"Could not locate required datasets in {h5_file.name}. Found lat={lat_path}, lon={lon_path}, aod={aod_path}"
                )
            lat = f[lat_path][:]
            lon = f[lon_path][:]
            aod = f[aod_path][:]

            # Parse date from filename like 3RIMG_01APR2025_0545_...
            import re, datetime as _dt
            m = re.search(r"_(\d{2}[A-Z]{3}\d{4})_", h5_file.name)
            if m:
                timestamp = _dt.datetime.strptime(m.group(1), "%d%b%Y").date()
            else:
                # Fallback to file modified date
                timestamp = pd.to_datetime(h5_file.stat().st_mtime, unit="s").date()

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

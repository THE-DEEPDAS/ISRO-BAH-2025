import pandas as pd
import xarray as xr
from pathlib import Path


# ======================
# USER CONFIG
# ======================
CPCB_FOLDER = Path("CPCB")          # CPCB daily CSV folder (site names come from filenames)
MERRA_FOLDER = Path("Merra-2")      # MERRA-2 hourly nc4 folder

# Coordinates provided by user (lat, lon) to map to CPCB sites by filename order
COORD_LIST = [
    (13.1278, 80.2642),
    (26.428282, 80.327067),
    (25.4547, 78.6039),
    (23.020509, 72.579261),
    (22.55664, 88.342674),
]


def build_site_coords(cpcb_folder: Path, coords: list[tuple[float, float]]):
    cpcb_sites = sorted([p.stem for p in cpcb_folder.glob("*.csv")])
    if len(cpcb_sites) != len(coords):
        raise ValueError(
            f"Expected {len(coords)} CPCB sites to map to coordinates, found {len(cpcb_sites)}: {cpcb_sites}"
        )
    return {name: coord for name, coord in zip(cpcb_sites, coords)}


def extract_daily_from_merra_folder(folder: Path, var_names: list[str], site_coords: dict[str, tuple[float, float]]):
    files = sorted(folder.glob("*.nc4"))
    if not files:
        raise FileNotFoundError(f"No .nc4 files found in MERRA folder: {folder}")

    # Open multiple files with a safe engine fallback
    paths = [str(p) for p in files]
    ds = None
    last_err = None
    for eng in ("netcdf4", "h5netcdf"):
        try:
            ds = xr.open_mfdataset(paths, combine="by_coords", engine=eng)
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
        # Ensure time is datetime and resample to daily means
        if "time" not in df.columns:
            raise RuntimeError("Expected a 'time' coordinate in MERRA dataset.")
        df["time"] = pd.to_datetime(df["time"])
        daily = df.set_index("time").resample("1D").mean().reset_index()
        daily["site"] = site
        daily.rename(columns={"time": "date"}, inplace=True)
        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        all_sites.append(daily)

    out = pd.concat(all_sites, ignore_index=True)
    return out


def main():
    site_coords = build_site_coords(CPCB_FOLDER, COORD_LIST)
    # Typical single-level diagnostics variables; adjust if needed
    merra_vars = ["T2M", "RH2M", "U2M", "V2M", "PS"]
    df = extract_daily_from_merra_folder(MERRA_FOLDER, merra_vars, site_coords)
    out_path = Path("merra_daily_features.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {out_path} with shape {df.shape}")


if __name__ == "__main__":
    main()

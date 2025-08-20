import pandas as pd
import xarray as xr
from pathlib import Path
import numpy as np

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


def _open_nc(path: Path) -> xr.Dataset:
    try:
        return xr.open_dataset(path, engine="netcdf4")
    except Exception as e1:
        try:
            return xr.open_dataset(path, engine="h5netcdf")
        except Exception as e2:
            raise RuntimeError(f"Failed to open {path.name} with netcdf4 or h5netcdf: {e1} | {e2}")


def _nearest_index(coord: np.ndarray, value: float) -> int:
    # coord expected 1D; works whether ascending or descending
    return int(np.nanargmin(np.abs(coord - value)))


def extract_daily_from_merra_folder(folder: Path, var_names: list[str], site_coords: dict[str, tuple[float, float]]):
    files = sorted(folder.glob("*.nc4"))
    if not files:
        raise FileNotFoundError(f"No .nc4 files found in MERRA folder: {folder}")

    # Inspect first file to map site -> (ilat, ilon)
    first = _open_nc(files[0])
    if "lat" not in first.coords or "lon" not in first.coords:
        raise RuntimeError("Expected 'lat' and 'lon' coordinates in MERRA files")
    lat_coord = np.asarray(first["lat"].values)
    lon_coord = np.asarray(first["lon"].values)
    if lat_coord.ndim != 1 or lon_coord.ndim != 1:
        first.close()
        raise RuntimeError("Expected 1D 'lat' and 'lon' coordinates in MERRA Nx grid")

    # Determine available variables once from first file
    requested_vars = list(var_names)
    # If RH2M requested but not present, try to compute it from QV2M, T2M, PS
    deps_for_rh = {"QV2M", "T2M", "PS"}
    need_compute_rh = ("RH2M" in requested_vars) and ("RH2M" not in first.data_vars)
    needed_vars = set(requested_vars)
    if need_compute_rh:
        needed_vars.update(deps_for_rh)

    avail_load_vars = [v for v in needed_vars if v in first.data_vars]
    missing_vars = [v for v in needed_vars if v not in first.data_vars]
    if missing_vars and all(v not in first.data_vars for v in requested_vars):
        raise RuntimeError(
            "None of the requested MERRA variables are present. Available variables include: "
            + ", ".join(list(first.data_vars.keys())[:30])
        )
    if missing_vars:
        print(f"⚠️ Missing MERRA variables in files (will skip): {missing_vars}")

    can_compute_rh = need_compute_rh and deps_for_rh.issubset(set(first.data_vars))

    site_idx: dict[str, tuple[int, int]] = {}
    for site, (lat, lon) in site_coords.items():
        ilat = _nearest_index(lat_coord, lat)
        ilon = _nearest_index(lon_coord, lon)
        site_idx[site] = (ilat, ilon)
    first.close()

    # Accumulate tiny per-file time series per site
    per_site_frames: dict[str, list[pd.DataFrame]] = {s: [] for s in site_coords}

    # helper to compute RH from QV2M, T2M, PS
    def _compute_rh2m(df: pd.DataFrame) -> pd.Series:
        # Inputs: QV2M (kg/kg), T2M (K), PS (Pa)
        if not {"QV2M", "T2M", "PS"}.issubset(df.columns):
            return pd.Series(index=df.index, dtype=float)
        q = pd.to_numeric(df["QV2M"], errors="coerce")
        T = pd.to_numeric(df["T2M"], errors="coerce")
        p = pd.to_numeric(df["PS"], errors="coerce")
        # e (Pa) from specific humidity
        e = q * p / (0.622 + 0.378 * q)
        Tc = T - 273.15
        es = 611.2 * np.exp(17.67 * Tc / (Tc + 243.5)) * 10  # 611.2 hPa? Keep consistent
        # Correction: 6.112 hPa -> 611.2 Pa; multiply by 100 for Pa
        es = 6.112 * 100.0 * np.exp(17.67 * Tc / (Tc + 243.5))
        rh = 100.0 * (e / es)
        rh = np.clip(rh, 0, 100)
        return pd.Series(rh, index=df.index)

    for p in files:
        ds = _open_nc(p)
        try:
            # Small dataset reduced to one grid cell, keep only requested variables
            small_vars = [v for v in avail_load_vars if v in ds.data_vars]
            if not small_vars:
                ds.close()
                continue
            for site, (ilat, ilon) in site_idx.items():
                sub: xr.Dataset = ds[small_vars].isel(lat=ilat, lon=ilon)
                # Load the tiny slice into memory, then detach from file
                sub = sub.load()
                df = sub.to_dataframe().reset_index()
                if "time" not in df.columns:
                    continue
                # synthesize RH2M if needed
                if can_compute_rh and "RH2M" in requested_vars and "RH2M" not in df.columns:
                    df["RH2M"] = _compute_rh2m(df)
                # Keep only requested outputs and time
                keep_cols = ["time"] + [c for c in requested_vars if c in df.columns]
                df = df[keep_cols]
                df["site"] = site
                per_site_frames[site].append(df)
        finally:
            ds.close()

    # Build daily means per site
    all_sites = []
    for site, frames in per_site_frames.items():
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        df["time"] = pd.to_datetime(df["time"])  # ensure datetime
        df = df.sort_values("time").drop_duplicates(subset=["time"])  # in case of overlaps
        daily = df.set_index("time").resample("1D").mean(numeric_only=True).reset_index()
        daily["site"] = site
        daily.rename(columns={"time": "date"}, inplace=True)
        daily["date"] = pd.to_datetime(daily["date"]).dt.date
        all_sites.append(daily)

    if not all_sites:
        raise RuntimeError("No time series extracted from MERRA files at the given sites.")

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

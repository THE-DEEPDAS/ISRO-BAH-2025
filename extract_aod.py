import pandas as pd
import numpy as np
from pathlib import Path

# ======================
# USER CONFIG
# ======================
CPCB_FOLDER = Path("CPCB")        # CPCB daily CSV folder (site names come from filenames)
MOSDAC_FOLDER = Path("mosdac")    # Folder with MOSDAC h5 files
AOD_DATASET_NAME = "AOD"          # Dataset name hint for AOD inside HDF5

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


def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2) ** 2 + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2)) * np.sin(dlon/2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


def _find_dataset(h5obj, name_hints=None, contains=None):
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
                if getattr(child, "shape", None) is not None and hasattr(child, "dtype"):
                    key_lower = k.lower()
                    if any(h.lower() == k.lower() for h in name_hints) or any(c in key_lower for c in contains):
                        return child_path
                else:
                    stack.append((child_path, child))
    return None


def extract_daily_from_h5(folder: Path, var_name: str, site_coords: dict[str, tuple[float, float]]):
    try:
        import h5py  # type: ignore
    except Exception as e:
        raise RuntimeError(
            f"h5py import failed ({e}). AOD (MOSDAC) extraction requires working h5py/HDF5. "
            "Fix by aligning h5py + hdf5 in your conda env."
        )

    files = sorted(folder.glob("*.h5"))
    if not files:
        raise FileNotFoundError(f"No MOSDAC .h5 files found in {folder}.")

    all_data = []
    import re, datetime as _dt
    for h5_file in files:
        with h5py.File(h5_file, "r") as f:
            lat_path = _find_dataset(f, name_hints=["Latitude"], contains=["lat"])
            lon_path = _find_dataset(f, name_hints=["Longitude"], contains=["lon"])
            aod_path = _find_dataset(f, name_hints=[var_name, "AOD"], contains=["aod"])
            if not (lat_path and lon_path and aod_path):
                raise KeyError(
                    f"Could not locate required datasets in {h5_file.name}. Found lat={lat_path}, lon={lon_path}, aod={aod_path}"
                )
            lat_raw = f[lat_path][:]
            lon_raw = f[lon_path][:]
            aod_raw = f[aod_path][:]

            # Squeeze potential singleton dimensions in AOD (e.g., (1, ny, nx))
            aod2 = np.squeeze(aod_raw)

            # Build 2D latitude/longitude grid aligned to AOD
            if lat_raw.ndim == 1 and lon_raw.ndim == 1:
                LAT, LON = np.meshgrid(lat_raw, lon_raw, indexing="ij")
            elif lat_raw.ndim == 2 and lon_raw.ndim == 2:
                LAT, LON = lat_raw, lon_raw
            else:
                # Try to broadcast to 2D
                try:
                    LAT, LON = np.broadcast_arrays(lat_raw, lon_raw)
                except Exception:
                    raise ValueError(
                        f"Unsupported lat/lon shapes: lat{lat_raw.shape} lon{lon_raw.shape} in {h5_file.name}"
                    )

            # If shapes don't match AOD, try transposing the AOD
            def pick_value(ii, jj, arr):
                if arr.ndim == 2 and arr.shape == LAT.shape:
                    return arr[ii, jj]
                if arr.ndim == 2 and arr.shape == (LAT.shape[1], LAT.shape[0]):
                    return arr[jj, ii]
                raise ValueError(
                    f"AOD shape {arr.shape} not compatible with LAT/LON {LAT.shape}; cannot index at {(ii, jj)}"
                )

            m = re.search(r"_(\d{2}[A-Z]{3}\d{4})_", h5_file.name)
            if m:
                timestamp = _dt.datetime.strptime(m.group(1), "%d%b%Y").date()
            else:
                timestamp = pd.to_datetime(h5_file.stat().st_mtime, unit="s").date()

            # Compute nearest pixel for each site
            for site, (s_lat, s_lon) in site_coords.items():
                # Great-circle distance to each grid cell
                dist = haversine(s_lat, s_lon, LAT, LON)
                ii, jj = np.unravel_index(np.argmin(dist), dist.shape)
                try:
                    val = pick_value(ii, jj, aod2)
                except ValueError:
                    # Try with flipped AOD if it had a leading singleton dim
                    if aod_raw.ndim == 3 and aod_raw.shape[0] == 1:
                        val = pick_value(ii, jj, aod_raw[0, ...])
                    else:
                        raise
                all_data.append({"site": site, "date": timestamp, var_name: val})

    df = pd.DataFrame(all_data)
    daily_df = df.groupby(["site", "date"], as_index=False).mean()
    return daily_df


def main():
    site_coords = build_site_coords(CPCB_FOLDER, COORD_LIST)
    df = extract_daily_from_h5(MOSDAC_FOLDER, AOD_DATASET_NAME, site_coords)
    out_path = Path("aod_daily_features.csv")
    df.to_csv(out_path, index=False)
    print(f"✅ Saved {out_path} with shape {df.shape}")


if __name__ == "__main__":
    main()

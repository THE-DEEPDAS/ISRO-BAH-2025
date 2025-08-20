# AOD and Reanalysis Extraction Guide

This guide explains what Aerosol Optical Depth (AOD) and reanalysis data are, how they’re structured in .h5 and .nc4 files, and exactly how this repository extracts daily features for model-ready CSVs.

## What are AOD and Reanalysis data?

- Satellite AOD (Aerosol Optical Depth)
  - Column-integrated measure of aerosols in the atmosphere (dimensionless). Higher AOD usually indicates more particles (dust, smoke, pollution) in the column.
  - Here we use INSAT/MOSDAC Level-2 AOD products with frequent observations (e.g., 30-minute cadence) in HDF5 (.h5) format.

- Reanalysis (MERRA-2)
  - A consistent, global, gridded reconstruction of the atmosphere produced by assimilating observations into a weather model. Provides hourly to sub-daily fields on a fixed grid.
  - Here we use MERRA-2 2D single-level hourly diagnostics in NetCDF4 (.nc4) format.

## Concepts used in extraction

- Spatial alignment by nearest grid point
  - Each CPCB site has a latitude/longitude. We pick the nearest pixel/grid cell from AOD and MERRA-2 to represent that site.

- Temporal aggregation to daily
  - Raw data are sub-daily (30-min AOD, hourly MERRA-2). We aggregate each site’s time series to a daily mean.

- Site–coordinate mapping
  - The scripts map CPCB sites to coordinates using a fixed list of (lat, lon) pairs in `COORD_LIST` and by sorting CPCB filenames (so site names remain stable across datasets).

## File formats and how we use them

- .h5 (HDF5) — used by MOSDAC/INSAT AOD
  - Hierarchical container with groups and datasets.
  - Stores arrays such as Latitude, Longitude, AOD, and metadata (attributes).
  - Accessed with `h5py`. We locate the relevant datasets (e.g., `AOD`, `Latitude`, `Longitude`) even if they are nested in groups.

- .nc4 (NetCDF-4/HDF5) — used by MERRA-2
  - Self-describing scientific format with variables, dimensions (time, lat, lon), coordinates, and attributes.
  - Accessed with `xarray` using `netcdf4` or `h5netcdf` engines. Variables include meteorology and surface diagnostics on a regular grid.

## How extraction works in this repo

- AOD extraction: `extract_aod.py`
  1. Discovers `.h5` files under `mosdac/` (or your configured folder).
  2. Opens each file via `h5py` and finds datasets for AOD, Latitude, Longitude.
  3. Handles different array shapes/orientations safely, builds 2D lat/lon if needed.
  4. For each CPCB site, picks the nearest AOD pixel for each observation timestamp.
  5. Parses the observation date from the filename (fallback to file mtime), then groups by day and averages.
  6. Writes `aod_daily_features.csv` with daily means per site.

- Reanalysis extraction: `extract_merra.py`
  1. Lists `.nc4` files under `Merra-2/`.
  2. Opens one file to resolve the grid and find the nearest (lat, lon) indices for every site.
  3. Iterates files and, for each site, slices a tiny time series at that single grid cell (so memory stays small).
  4. Concatenates per-site time series, deduplicates any overlapping timestamps, and resamples to daily means.
  5. Writes `merra_daily_features.csv` with daily means per site.

## Running the scripts (Windows PowerShell)

- Ensure dependencies are installed in your Python environment:
  - AOD: `h5py`
  - MERRA-2: `xarray` plus either `netcdf4` or `h5netcdf`, and `cftime`
- Then run:

```powershell
python extract_aod.py
python extract_merra.py
```

Output CSVs are saved in the repo root as `aod_daily_features.csv` and `merra_daily_features.csv`.

## Variable names, units, and domain knowledge

- AOD (unitless)
  - Column aerosol loading, typically at 550 nm. Higher AOD correlates with more aerosols but is influenced by humidity, vertical distribution, and retrieval conditions (e.g., clouds).

- MERRA-2 variables (from hourly `M2T1NXSLV` files):
  - T2M (Kelvin): 2-meter air temperature.
  - U2M (m/s): 2-meter wind component (zonal, positive eastward).
  - V2M (m/s): 2-meter wind component (meridional, positive northward).
  - PS (Pa): Surface pressure.
  - RH2M (%, optional): 2-meter relative humidity. Note: Some MERRA-2 collections may not include RH2M. In that case, it can be derived from T2M, PS, and QV2M (specific humidity at 2 m) if QV2M is available. The scripts currently skip RH2M if not present.

- Coordinates and time
  - lat, lon: Grid coordinates (MERRA-2 ~0.5° x 0.625°). We use nearest-neighbor selection to match CPCB site locations.
  - time/date: Source data time is UTC; daily features are calendar-day aggregates.

- CPCB fields (ground truth; not extracted by these scripts but used for site naming/merging)
  - Each raw CPCB CSV contains 15-min measurements (e.g., PM2.5/PM10/CO/SO2). The final model pipeline merges CPCB daily aggregates with AOD and MERRA-2 daily features by site and date.

## Practical tips and caveats

- Missing variables
  - If a requested MERRA-2 variable isn’t present (e.g., `RH2M`), the extractor logs and skips it. You can add a derivation later if needed.

- Retrieval differences for AOD
  - Cloud, sun–glint, or low surface reflectance can reduce coverage. Daily means average available observations.

- Engines and drivers
  - On Windows, ensure compatible versions of `netcdf4`/`h5netcdf` and `h5py` to avoid DLL errors. If one engine fails, the scripts try alternatives where possible.

## Outputs

- `aod_daily_features.csv`
  - Columns: `date` (YYYY-MM-DD), `site` (CPCB site name), `AOD` (daily mean).

- `merra_daily_features.csv`
  - Columns: `date`, `site`, and one column per available MERRA-2 variable (e.g., `T2M`, `U2M`, `V2M`, `PS`, optionally `RH2M`). Values are daily means at the site’s nearest grid cell.

## Reproducibility

- Site mapping comes from CPCB file stems sorted lexicographically and mapped to coordinates in `COORD_LIST` within the scripts. Keep the order consistent when changing coordinates or adding sites.

---

If you need RH2M but it’s not in your MERRA-2 files, consider adding a small post-processing step to compute RH from T, P, and Q (specific humidity). This can be added later without altering the core extraction flow above.

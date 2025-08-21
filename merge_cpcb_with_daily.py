import sys
from pathlib import Path
import pandas as pd

CPCB_DIR = Path("CPCB")
AOD_CSV = Path("aod_daily_features.csv")
MERRA_CSV = Path("merra_daily_features.csv")
OUT_DIR = CPCB_DIR  # write merged files next to the originals


def load_daily(csv_path: Path, name: str) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing {name} daily features file: {csv_path}")
    df = pd.read_csv(csv_path)
    if "date" not in df.columns:
        raise ValueError(f"Expected a 'date' column in {csv_path}")
    # Normalize to date type
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if "site" in df.columns:
        df["site"] = df["site"].astype(str).str.strip()
    return df


def main():
    aod = load_daily(AOD_CSV, "AOD")
    merra = load_daily(MERRA_CSV, "MERRA")

    cpcb_files = sorted(CPCB_DIR.glob("*.csv"))
    if not cpcb_files:
        print(f"No CPCB files found in {CPCB_DIR}")
        return

    for cpcb_file in cpcb_files:
        site = cpcb_file.stem  
        print(f"Merging for site: {site}")
        df = pd.read_csv(cpcb_file)

        # Use 'period.datetimeTo.utc' per instruction, fallback to 'period.datetimeFrom.utc' if missing
        if "period.datetimeTo.utc" in df.columns:
            date_col = "period.datetimeTo.utc"
        elif "period.datetimeFrom.utc" in df.columns:
            date_col = "period.datetimeFrom.utc"
            print(f"  Note: {cpcb_file.name} missing 'period.datetimeTo.utc'; using 'period.datetimeFrom.utc' instead")
        else:
            print(f"  Skipping {cpcb_file.name}: no 'period.datetimeTo.utc' or 'period.datetimeFrom.utc' column")
            continue

        # Create a 'date' column (UTC calendar day) and a normalized 'site' for joining
        df["date"] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.date
        df["site"] = str(site).strip()

        # Ensure feature frames have 'site' and unique (site, date); filter to current site
        if "site" not in aod.columns or "site" not in merra.columns:
            raise ValueError("Both AOD and MERRA daily features must include a 'site' column matching CPCB filenames.")
        aod_site = aod[aod["site"].astype(str).str.strip() == df["site"].iloc[0]].drop_duplicates(subset=["date"]).copy()
        merra_site = merra[merra["site"].astype(str).str.strip() == df["site"].iloc[0]].drop_duplicates(subset=["date"]).copy()
        # Drop 'site' in feature frames to avoid duplicate columns after merge
        aod_site = aod_site.drop(columns=["site"], errors="ignore")
        merra_site = merra_site.drop(columns=["site"], errors="ignore")

        # Merge by date only (left join keeps all CPCB rows)
        merged = df.merge(aod_site, on=["date"], how="left")
        merged = merged.merge(merra_site, on=["date"], how="left")

        out_path = OUT_DIR / f"{site}_merged.csv"
        merged.to_csv(out_path, index=False)
        print(f"  -> wrote {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)

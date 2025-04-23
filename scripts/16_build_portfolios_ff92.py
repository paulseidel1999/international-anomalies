# scripts/05_build_portfolios_ff92.py
"""
Build FF92-based anomaly portfolios for each fiscal-year bin and country.
Reads the wide anomalies table, joins to returns, and loops over all anomaly columns.
"""
import os
from pathlib import Path
import polars as pl

# limit threads for stability
os.environ["RAYON_NUM_THREADS"] = "1"
os.environ["POLARS_MAX_THREADS"] = "1"

# ----------------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ANOMALY_PATH  = PROJECT_ROOT / "data" / "processed" / "anomalies_worldscope.parquet"
DS_PATH       = PROJECT_ROOT / "data" / "processed" / "Datastream_with_matching.parquet"
OUTPUT_DIR    = PROJECT_ROOT / "data" / "processed" / "portfolios_ff92"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    # 1) Load anomalies and returns
    df_ano = pl.read_parquet(ANOMALY_PATH)
    df_ds  = pl.read_parquet(DS_PATH)

        # determine anomaly columns (exclude key + date fields, keep only numeric vars)
    schema = df_ano.schema
    meta_cols = {"ws_id","point_date","freq","fiscal_period","cal1_55350","pit_date","fye_date","ff92_date"}
    anomalies = [c for c,dt in schema.items() if c not in meta_cols and dt in (pl.Float64, pl.Int64)]
    if not anomalies:
        print("⚠️  No numeric anomaly columns to process, exiting.")
        return
    print(f"Found {len(anomalies)} anomalies: {anomalies}")

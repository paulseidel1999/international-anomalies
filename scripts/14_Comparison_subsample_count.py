import os
import polars as pl
from pathlib import Path

def main():
    # Force Polars/Rayon to single-thread
    os.environ["RAYON_NUM_THREADS"] = "1"
    os.environ["POLARS_MAX_THREADS"] = "1"

    # Paths
    project_root = Path(__file__).resolve().parent.parent
    input_file = project_root / "data" / "interim" / "Worldscope_clean" / "WSFV_merged_20250131_final_no_cols.parquet"

    # Read data
    df = pl.read_parquet(input_file)

    # Parse fiscal-year-end date and PIT release date
    df = df.with_columns([
        pl.col("cal1_55350")
          .str.replace_all('"', '')
          .str.replace(r"^d", "")
          .str.strptime(pl.Date, format="%Y%m%d")
          .alias("fye_date"),
        pl.col("point_date").dt.date().alias("pit_date"),
    ])

    # Compute FF92 date (6 months after FYE)
    df = df.with_columns([
        pl.col("fye_date").dt.offset_by("6mo").alias("ff92_date")
    ])

    # Compute days difference between PIT and FF92
    df = df.with_columns([
        (pl.col("pit_date").cast(pl.Int64) - pl.col("ff92_date").cast(pl.Int64))
        .alias("diff_to_ff92")
    ])

    # Count observations before/on and after FF92 date
    count_before = df.filter(pl.col("diff_to_ff92") <= 0).height
    count_after  = df.filter(pl.col("diff_to_ff92") > 0).height

    # Print results
    print(f"Observations on or before FF92 date: {count_before}")
    print(f"Observations after FF92 date: {count_after}")

if __name__ == "__main__":
    main()

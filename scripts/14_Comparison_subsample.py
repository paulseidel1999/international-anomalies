# scripts/14_PIT_vs_FF92_filter.py

import os
from pathlib import Path
import polars as pl

# Lookup table for 5-year bins
def make_year_bins() -> pl.DataFrame:
    years = list(range(1970, 2030))
    bins = []
    for y in years:
        if 1990 <= y <= 1994:
            bins.append("1990–1994")
        elif 1995 <= y <= 1999:
            bins.append("1995–1999")
        elif 2000 <= y <= 2004:
            bins.append("2000–2004")
        elif 2005 <= y <= 2009:
            bins.append("2005–2009")
        elif 2010 <= y <= 2014:
            bins.append("2010–2014")
        elif 2015 <= y <= 2018:
            bins.append("2015–2018")
        else:
            bins.append("Other")
    return pl.DataFrame({"fye_year": years, "fye_bin": bins})


def main():
    # force single-thread for stability
    os.environ["RAYON_NUM_THREADS"] = "1"
    os.environ["POLARS_MAX_THREADS"] = "1"

    # Paths
    project_root = Path(__file__).resolve().parent.parent
    in_file = project_root / "data" / "interim" / "Worldscope_clean" / "WSFV_merged_20250131_final_no_cols.parquet"
    out_dir = project_root / "data" / "interim" / "Worldscope_clean_panels"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Read base data
    df = pl.read_parquet(in_file)

    # Parse dates
    df = df.with_columns([
        pl.col("cal1_55350")
          .str.replace_all('"', '')
          .str.replace(r"^d", "")
          .str.strptime(pl.Date, format="%Y%m%d")
          .alias("fye_date"),
        pl.col("point_date").dt.date().alias("pit_date"),
    ])

    # FF92 date
    df = df.with_columns(
        pl.col("fye_date").dt.offset_by("6mo").alias("ff92_date")
    )

    # Compute day differences and fiscal year
    df = df.with_columns([
        (pl.col("pit_date").cast(pl.Int32) - pl.col("fye_date").cast(pl.Int32)).alias("diff_to_fye"),
        (pl.col("pit_date").cast(pl.Int32) - pl.col("ff92_date").cast(pl.Int32)).alias("diff_to_ff92"),
        pl.col("fye_date").dt.year().alias("fye_year"),
    ])

    # Map fiscal years to bins via join
    bins_df = make_year_bins()
    df = df.join(bins_df, on="fye_year", how="left")

    # Common aggregations on diff_to_ff92
    aggs = [
        pl.mean("diff_to_ff92").round(1).alias("Mean"),
        pl.median("diff_to_ff92").alias("Median"),
        pl.std("diff_to_ff92").alias("StdDev"),
        pl.col("diff_to_ff92").quantile(0.10).alias("10thPct"),
        pl.col("diff_to_ff92").quantile(0.90).alias("90thPct"),
    ]

    # Panel: PIT after FF92 date
    panel_after = (
        df.filter(pl.col("diff_to_ff92") > 0)
          .group_by("fye_bin")
          .agg(aggs)
          .sort("fye_bin")
    )
    panel_after.write_csv(out_dir / "panel_after_ff92_only.csv")

    # Panel: PIT on or before FF92 date
    panel_before = (
        df.filter(pl.col("diff_to_ff92") <= 0)
          .group_by("fye_bin")
          .agg(aggs)
          .sort("fye_bin")
    )
    panel_before.write_csv(out_dir / "panel_before_ff92_only.csv")

    print("Panels written to:", out_dir.resolve())

if __name__ == "__main__":
    main()

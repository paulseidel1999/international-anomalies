# scripts/13_Comparison_PITvsFF92.py

import os
# Force Polars/Rayon to single-threaded mode
os.environ["RAYON_NUM_THREADS"] = "1"
os.environ["POLARS_MAX_THREADS"] = "1"

import polars as pl
from pathlib import Path


def year_bins_df() -> pl.DataFrame:
    """Build a lookup DataFrame mapping fiscal years to 5-year bins."""
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
    # 1) Paths
    project_root = Path(__file__).resolve().parent.parent
    in_file = project_root / "data" / "interim" / "Worldscope_clean" / "WSFV_merged_20250131_final_no_cols.parquet"
    out_dir = project_root / "data" / "interim" / "Worldscope_clean_panels"
    out_dir.mkdir(parents=True, exist_ok=True)

    # 2) Load
    df = pl.read_parquet(in_file)

    # 3) Parse dates
    df = df.with_columns([
        pl.col("cal1_55350")
          .str.replace_all('"', '')
          .str.replace(r"^d", "")
          .str.strptime(pl.Date, format="%Y%m%d")
          .alias("fye_date"),
        pl.col("point_date").dt.date().alias("pit_date"),
    ])

    # 4) Compute FF92 date
    df = df.with_columns([
        pl.col("fye_date").dt.offset_by("6mo").alias("ff92_date")
    ])

    # 5) Day differences and fiscal year
    df = df.with_columns([
        (pl.col("pit_date").cast(pl.Int64) - pl.col("fye_date").cast(pl.Int64)).alias("diff_to_fye"),
        (pl.col("pit_date").cast(pl.Int64) - pl.col("ff92_date").cast(pl.Int64)).alias("diff_to_ff92"),
        pl.col("fye_date").dt.year().alias("fye_year"),
    ])

    # 6) Join on 5-year bins
    bins_df = year_bins_df()
    df = df.join(bins_df, on="fye_year", how="left")

    # 7) Panel A: PIT → FYE distribution
    panel_a = (
        df.group_by("fye_bin").agg([
            pl.mean("diff_to_fye").round(1).alias("Mean"),
            pl.median("diff_to_fye").alias("Median"),
            pl.std("diff_to_fye").alias("StdDev"),
            pl.col("diff_to_fye").quantile(0.10).alias("10thPct"),
            pl.col("diff_to_fye").quantile(0.90).alias("90thPct"),
        ]).sort("fye_bin")
    )
    panel_a.write_csv(out_dir / "panel_A_diff_pit_to_fye.csv")

    # 8) Panel B: PIT → FF92 distribution
    panel_b = (
        df.group_by("fye_bin").agg([
            pl.mean("diff_to_ff92").round(1).alias("Mean"),
            pl.median("diff_to_ff92").alias("Median"),
            pl.std("diff_to_ff92").alias("StdDev"),
            pl.col("diff_to_ff92").quantile(0.10).alias("10thPct"),
            pl.col("diff_to_ff92").quantile(0.90).alias("90thPct"),
        ]).sort("fye_bin")
    )
    panel_b.write_csv(out_dir / "panel_B_diff_pit_to_ff92.csv")

        # 9) Panel C: FF92 availability at rebalancing
    total      = pl.count().alias("N")
    late_count = (pl.col("diff_to_ff92").gt(0).cast(pl.Int64).sum().alias("FF92_not_available"))
    avail_count= (pl.col("diff_to_ff92").le(0).cast(pl.Int64).sum().alias("Data_available_by_FF92"))
    pct_late   = (late_count / total * 100).round(1).alias("Pct_FF92_not_avail")
    pct_avail  = (avail_count / total * 100).round(1).alias("Pct_data_avail_by_FF92")

    panel_c = (
        df.group_by("fye_bin").agg([total, late_count, pct_late, avail_count, pct_avail]).sort("fye_bin")
    )
    panel_c.write_csv(out_dir / "panel_C_availability_at_FF92.csv")

    print("Panels written to:", out_dir.resolve())("Panels written to:", out_dir.resolve())

if __name__ == "__main__":
    main()

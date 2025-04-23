import polars as pl
from pathlib import Path

def main():
    # --------------------------------------------------------------------------
    # 1) Define paths
    # --------------------------------------------------------------------------
    root_dir = Path(__file__).resolve().parents[1]
    
    main_file = root_dir / "data" / "interim" / "worldscope" / "WSFV_f_20250131.parquet"
    cal_file1 = root_dir / "data" / "interim" / "worldscope" / "WSCalendarPrd_f_20250131.parquet"
    cal_file2 = root_dir / "data" / "interim" / "worldscope" / "WSReportedPrd_f_20250131.parquet"

    output_file = root_dir / "data" / "interim" / "Worldscope_clean" / "WSFV_merged_20250131.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------------
    # 2) Read Main WSFV Data
    #    The main file has columns: [ws_id, point_date, freq, fiscal_period, item_code, value, ...]
    # --------------------------------------------------------------------------
    main_df = pl.read_parquet(main_file)

    # --------------------------------------------------------------------------
    # PART A: Process WSCalendarPrd_f_20250131
    # Pivot by (ws_id, point_date, freq, fiscal_period) where each distinct item_code becomes a column.
    # --------------------------------------------------------------------------
    cal1_df = pl.read_parquet(cal_file1)

    # Aggregate to ensure one value per key (ws_id, point_date, freq, fiscal_period, item_code)
    cal1_prep = cal1_df.group_by(["ws_id", "point_date", "freq", "fiscal_period", "item_code"]).agg(
        pl.first("value").alias("value")
    )
    # Pivot the aggregated data: each unique item_code becomes a column.
    cal1_wide = cal1_prep.pivot(
        index=["ws_id", "point_date", "freq", "fiscal_period"],
        columns="item_code",
        values="value"
    )
    # Rename pivoted columns to add the prefix "cal1_"
    col_map = {}
    for col in cal1_wide.columns:
        if col not in ["ws_id", "point_date", "freq", "fiscal_period"]:
            col_map[col] = f"cal1_{col}"
    cal1_wide = cal1_wide.rename(col_map)

    # Join with the main DataFrame on the 4 key columns
    merged1 = main_df.join(
        cal1_wide,
        left_on=["ws_id", "point_date", "freq", "fiscal_period"],
        right_on=["ws_id", "point_date", "freq", "fiscal_period"],
        how="left"
    )

    # --------------------------------------------------------------------------
    # PART B: Process WSReportedPrd_f_20250131
    # Pivot by (ws_id, point_date, freq) where each distinct item_code becomes a column.
    # --------------------------------------------------------------------------
    cal2_df = pl.read_parquet(cal_file2)

    # Aggregate to ensure one value per key (ws_id, point_date, freq, item_code)
    cal2_prep = cal2_df.group_by(["ws_id", "point_date", "freq", "item_code"]).agg(
        pl.first("value").alias("value")
    )
    # Pivot the aggregated data
    cal2_wide = cal2_prep.pivot(
        index=["ws_id", "point_date", "freq"],
        columns="item_code",
        values="value"
    )
    # Rename pivoted columns to add the prefix "cal2_"
    col_map2 = {}
    for col in cal2_wide.columns:
        if col not in ["ws_id", "point_date", "freq"]:
            col_map2[col] = f"cal2_{col}"
    cal2_wide = cal2_wide.rename(col_map2)

    # 2nd join on the keys for the reported period data.
    merged2 = merged1.join(
        cal2_wide,
        left_on=["ws_id", "point_date", "freq"],
        right_on=["ws_id", "point_date", "freq"],
        how="left"
    )

    # --------------------------------------------------------------------------
    # 4) Write final merged dataset
    # --------------------------------------------------------------------------
    merged2.write_parquet(output_file)
    print(f"Done. Wrote merged file to: {output_file}")
    print(f"Final shape: {merged2.shape}")

if __name__ == "__main__":
    main()

import polars as pl
from pathlib import Path

def main():
    root_dir = Path(__file__).resolve().parents[1]
    
    # 1) Path to the Datastream file containing 'WC06105'
    ds_file = root_dir / "data" / "processed" / "Datastream_with_matching.parquet"
    
    # 2) Path to the worldscope item=2003 file
    ws_file = root_dir / "data" / "interim" / "worldscope_items" / "WS_item_2003.parquet"
    
    # 3) Output path
    output_file = root_dir / "data" / "processed" / "DS_with_WS2003.parquet"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 4) Read the Datastream data
    ds = pl.read_parquet(ds_file)
    # ds should have columns like: ["DSCode", "Date", "RI", "Currency", "MV", "GEOGC", "WC06105", ...]

    # 5) Read the worldscope file for item_code=2003
    ws = pl.read_parquet(ws_file)
    # ws has columns like: ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "value"]

    # 6) Convert the worldscope "point_date" from datetime to date
    #    That way it matches Datastream's "Date" (which is typically pl.Date)
    ws = ws.with_columns(
        pl.col("point_date").dt.date()  # extracts the date portion (no time)
    )

    # (Optional) rename "value" to something more descriptive before the join
    # Or we can rename after. We’ll do it after so we can see the original name first.

    # 7) Join on WC06105==ws_id AND Date==point_date
    merged = ds.join(
        ws.select(["ws_id", "point_date", "value"]),  # only these columns needed for the join
        left_on=["WC06105", "Date"],
        right_on=["ws_id", "point_date"],
        how="left"
    )

    # 8) Rename "value" -> "WS2003" (or any name you prefer)
    merged = merged.rename({"value": "WS2003_PIT"})

    # 9) Write out the final dataset
    merged.write_parquet(output_file)
    
    print(f"Merged file written to: {output_file}")
    print(f"Result shape: {merged.shape}")

if __name__ == "__main__":
    main()

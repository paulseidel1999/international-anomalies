import polars as pl
from pathlib import Path

def main():
    root_dir = Path(__file__).resolve().parents[1]  # your project root
    ds_path = root_dir / "data" / "interim" / "datastream" / "Datastream_consolidated.parquet"
    matching_path = root_dir / "data" / "interim" / "universal matching file" / "UniverseMatchingFile_consolidated.parquet"
    
    # Output in "data/processed" folder under your project
    output_dir = root_dir / "data" / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)  # create if needed
    output_file = output_dir / "Datastream_with_matching.parquet"

    # 1) Read Datastream
    ds = pl.read_parquet(ds_path)

    # 2) Read matching
    match_df = pl.read_parquet(matching_path)
    # keep only relevant columns: DSCD, GEOGC, WC06105
    match_df = match_df.select(["DSCD", "GEOGC", "WC06105"])

    # 3) Join on DSCode == DSCD, left join
    combined = ds.join(
        match_df,
        left_on="DSCode",
        right_on="DSCD",
        how="left"
    )

    # 4) Write result
    combined.write_parquet(output_file)
    print(f"Joined file written to: {output_file}")
    print(f"Result shape: {combined.shape}")

if __name__ == "__main__":
    main()

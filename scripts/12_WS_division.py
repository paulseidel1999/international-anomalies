import polars as pl
from pathlib import Path

def main():
    # --------------------------------------------------------------------------
    # 1) Define paths
    # --------------------------------------------------------------------------
    root_dir = Path(__file__).resolve().parents[1]
    input_file = (
        root_dir
        / "data"
        / "interim"
        / "Worldscope_clean"
        / "WSFV_merged_20250131_final_no_cols.parquet"
    )
    output_dir = root_dir / "data" / "interim" / "Worldscope_clean_items"
    output_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------------------
    # 2) Read the full dataset
    # --------------------------------------------------------------------------
    df = pl.read_parquet(input_file)

    # --------------------------------------------------------------------------
    # 3) Gather all distinct item_code values
    # --------------------------------------------------------------------------
    item_codes = df["item_code"].unique().to_list()
    print(f"Found {len(item_codes)} distinct item_code(s).")

    # --------------------------------------------------------------------------
    # 4) Split & write one file per code
    # --------------------------------------------------------------------------
    for code in item_codes:
        subset = df.filter(pl.col("item_code") == code)
        if subset.is_empty():
            continue

        output_file = output_dir / f"WS_item_{code}.parquet"
        subset.write_parquet(output_file)
        print(f"→ Wrote {subset.height} rows for item_code={code} → {output_file}")

if __name__ == "__main__":
    main()

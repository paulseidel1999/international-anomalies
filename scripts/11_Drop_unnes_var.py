import polars as pl
from pathlib import Path

def main():
    # Path to the original Parquet file
    input_file = Path("data/interim/Worldscope_clean/WSFV_merged_20250131_final.parquet")
    
    # Define a name for the new Parquet file
    output_file = input_file.parent / "WSFV_merged_20250131_final_no_cols.parquet"
    
    # Read the original DataFrame
    df = pl.read_parquet(input_file)
    print(f"Original columns: {df.columns}")
    
    # Columns to drop
    cols_to_drop = ["cal1_57034", "cal1_55352", "cal1_55555", "cal2_55558"]
    
    # Only drop the columns that exist in the DataFrame
    existing_cols_to_drop = [col for col in cols_to_drop if col in df.columns]
    df_new = df.drop(existing_cols_to_drop)
    
    # Write the new DataFrame to a new Parquet file
    df_new.write_parquet(output_file)
    
    # Print summary information
    print(f"Dropped columns: {existing_cols_to_drop}")
    print(f"New file saved at: {output_file.resolve()}")
    print(f"New columns: {df_new.columns}")

if __name__ == "__main__":
    main()

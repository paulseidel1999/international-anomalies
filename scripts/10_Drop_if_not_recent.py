import polars as pl
from pathlib import Path

def main():
    # Define input and output file paths
    input_file = Path("data/interim/Worldscope_clean/WSFV_merged_20250131_filtered.parquet")
    # Use the parent directory of input_file and add the new filename
    output_file = input_file.parent / "WSFV_merged_20250131_final.parquet"

    # Read the filtered Parquet file
    df = pl.read_parquet(input_file)
    
    # Filter the DataFrame to keep only rows where cal1_55555 equals cal2_55559.
    # Adjust the comparison (i.e., numeric or string) if necessary
    df_final = df.filter(pl.col("cal1_55555") == pl.col("cal2_55559"))
    
    # Save the final DataFrame to a new Parquet file
    df_final.write_parquet(output_file)
    
    print(f"Final filtered file saved at: {output_file.resolve()}")
    print(f"Original shape: {df.shape}, Final shape: {df_final.shape}")

if __name__ == "__main__":
    main()

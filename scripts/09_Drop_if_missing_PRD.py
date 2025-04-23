import polars as pl
from pathlib import Path

def main():
    # Path to the original Parquet file
    input_file = Path("data/interim/Worldscope_clean/WSFV_merged_20250131.parquet")
    # Path for the filtered Parquet file (saved in the same folder)
    output_file = Path("data/interim/Worldscope_clean/WSFV_merged_20250131_filtered.parquet")

    # Read the Parquet file
    df = pl.read_parquet(input_file)

    # Filter out rows where both columns "cal2_55558" and "cal2_55559" are "0"
    # (If they are numeric columns, compare with == 0 instead)
    df_filtered = df.filter(
        ~((pl.col("cal2_55558") == "0") & (pl.col("cal2_55559") == "0"))
    )

    # Write the filtered DataFrame to a new Parquet file
    df_filtered.write_parquet(output_file)
    print(f"Filtered file created at: {output_file.resolve()}")
    print(f"Original shape: {df.shape}, Filtered shape: {df_filtered.shape}")

if __name__ == "__main__":
    main()

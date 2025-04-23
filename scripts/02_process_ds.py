import polars as pl
import pathos.multiprocessing as mp

from tqdm import tqdm
from pathlib import Path
from loguru import logger

class DatastreamProcessor:
   def __init__(self):
       self.root_dir = Path(__file__).resolve().parents[1]
       self.raw_dir = self.root_dir / "data" / "raw"
       self.interim_dir = self.root_dir / "data" / "interim"

       # Adjusted here to point to your actual Datastream folder:
       self.ds_dir = self.raw_dir / "Anomaly Publication" / "Data" / "Datastream"
       
       self.interim_dir.mkdir(parents=True, exist_ok=True)
       self.output_dir = self.interim_dir / "datastream"
       self.output_dir.mkdir(exist_ok=True)
       
       self.data_dirs = [
           self.ds_dir / "Daily Index Returns LC",
           self.ds_dir / "Daily Index Returns USD",
           self.ds_dir / "Daily MV LC",
           self.ds_dir / "Daily MV USD",
           self.ds_dir / "Daily Returns LC",
           self.ds_dir / "Daily Returns USD"
       ]
       
   def find_csv_files(self):
       all_csvs = [file for folder in self.data_dirs for file in folder.rglob("*.csv")]
       logger.info(f"Found {len(all_csvs)} CSV files to process")
       return all_csvs
   
   def process_file(self, filepath):
       try:
           file_dir = filepath.parent.name
           
           # Skip any file in a folder named "Index Returns"
           if not "Index Returns" in file_dir:
               return self.process_non_index_file(filepath)
           else:
               return (False, filepath, "Index file", 0)
               
       except Exception as e:
           logger.error(f"Error processing {filepath}: {str(e)}")
           return (False, filepath, str(e), 0)

   def process_non_index_file(self, filepath):
       try:
           file_path = Path(filepath)
           file_name = file_path.stem
           
           currency = "Unknown"
           path_str = str(filepath)
           if "LC" in path_str:
               currency = "LC"
           elif "USD" in path_str:
               currency = "USD"
           
           # 1) Restrict Polars to 1 thread
           df = pl.read_csv(
               filepath,
               encoding="us-ascii",
               skip_rows=1,
               quote_char='"',
               infer_schema_length=0,
               n_threads=1  # <-- ADDED
           )
           
           meta_cols = ["DATES"]
           date_cols = df.columns[7:]
           value_col_name = "MV" if "MV" in file_name else "RI"
           
           result_df = (
               df.melt(
                   id_vars=meta_cols,
                   value_vars=date_cols,
                   variable_name="Date",
                   value_name="Value"
               )
               .with_columns([
                   pl.lit(currency).alias("Currency"),
                   pl.col("Value").cast(pl.Float64, strict=False),
                   pl.col("Date").str.extract(r"^(\d{2}/\d{2}/\d{4}).*", 1).alias("CleanDate")
               ])
               .with_columns([
                   pl.when(pl.col("CleanDate") == "")
                   .then(pl.col("Date"))
                   .otherwise(pl.col("CleanDate"))
                   .alias("CleanDate")
               ])
               .rename({"DATES": "DSCode", "Value": value_col_name})
               .with_columns([
                   pl.col("DSCode").cast(pl.Utf8),
                   pl.col("Currency").cast(pl.Utf8)
               ])
           )

           # 2) Convert the date string if possible
           try:
               result_df = (
                   result_df
                   .with_columns([
                       pl.col("CleanDate").str.strptime(pl.Date, "%d/%m/%Y", strict=True).alias("Date")
                   ])
                   .drop("CleanDate")
               )
           except Exception as date_err:
               logger.warning(f"Error converting dates: {str(date_err)}")
               result_df = result_df.rename({"CleanDate": "Date"})
           
           if result_df.height == 0:
               logger.warning(f"No data found in {filepath}")
               return (False, filepath, "No data rows found", 0)
           
           output_path = self.output_dir / f"{file_name}.parquet"
           result_df.write_parquet(output_path)
           
           return (True, filepath, None, result_df.height)
           
       except Exception as e:
           logger.error(f"Error processing {filepath}: {str(e)}")
           return (False, filepath, str(e), 0)
           
   def run(self):
       logger.info("Starting Datastream CSV processing")
       
       if not self.ds_dir.exists():
           logger.error(f"Datastream directory not found: {self.ds_dir}")
           return
       
       csv_files = self.find_csv_files()
       
       if not csv_files:
           logger.warning("No CSV files found!")
           return
       
       # 3) Use fewer processes to avoid resource limits
       num_processes = min(mp.cpu_count(), 4)  # <-- CHANGED from mp.cpu_count() to 4
       logger.info(f"Using {num_processes} CPU cores for parallel processing")
       
       success_count = 0
       total_rows = 0
       
       # Process CSVs in parallel, but with limited concurrency
       with mp.Pool(processes=num_processes) as pool:
           with tqdm(total=len(csv_files), desc="Processing CSV files") as pbar:
               for success, _, _, row_count in pool.imap_unordered(self.process_file, csv_files):
                   if success:
                       success_count += 1
                       if isinstance(row_count, tuple):
                           total_rows += sum(row_count)
                       else:
                           total_rows += row_count
                   pbar.update(1)
       
       logger.success(f"Processing complete: {success_count}/{len(csv_files)} files processed successfully")
       logger.info(f"Total rows processed: {total_rows:,}")
       
       return success_count, len(csv_files)

def main():
   processor = DatastreamProcessor()
   processor.run()

if __name__ == "__main__":
   main()

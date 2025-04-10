import concurrent.futures
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from tqdm import tqdm

class MatchingFileProcessor:
    def __init__(self):
        self.root_dir = Path(__file__).resolve().parents[1]
        self.raw_dir = self.root_dir / "data" / "raw"
        self.interim_dir = self.root_dir / "data" / "interim"
        self.matching_dir = self.raw_dir / "Datastream" / "Universal Matching File"  # Path to matching files

        self.interim_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir = self.interim_dir / "universal matching file"
        self.output_dir.mkdir(exist_ok=True)

        self.columns_to_keep = ["DSCD", "ISIN", "LOC", "GEOGC", "WC06105"]

    def find_csv_files(self):
        csv_files = list(self.matching_dir.glob("*.csv"))
        csv_files = [csv for csv in csv_files if not os.path.getsize(csv) == 0]
        logger.info(f"Found {len(csv_files)} matching CSV files to process")
        return csv_files

    def convert_to_parquet(self, input_file_path, chunk_size=100_000):
        input_file_path = Path(input_file_path)
        input_filename = input_file_path.stem
        output_file = self.output_dir / f"{input_filename}.parquet"

        writer = None
        total_rows = 0

        try:
            for chunk_df in pd.read_csv(
                input_file_path,
                chunksize=chunk_size,
                dtype_backend='pyarrow',
                encoding="windows-1252"
            ):

                table = pa.Table.from_pandas(chunk_df, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(output_file, table.schema, compression='zstd')

                writer.write_table(table)
                total_rows += len(chunk_df)

            if writer:
                writer.close()

            return (True, input_file_path, None, total_rows)

        except Exception as e:
            logger.error(f"Error processing {input_file_path}: {e}")
            return (False, input_file_path, str(e), 0)
        finally:
            if writer:
                writer.close()

    def run(self):
        logger.info("Starting Matching file processing")

        if not self.matching_dir.exists():
            logger.error(f"Matching directory not found: {self.matching_dir}")
            return

        csv_files = self.find_csv_files()
        if not csv_files:
            logger.warning("No CSV files found!")
            return

        success_count = 0
        total_rows = 0

        max_threads = os.cpu_count()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            with tqdm(total=len(csv_files), desc="Processing Matching CSV files") as pbar:
                future_to_file = {executor.submit(self.convert_to_parquet, file): file for file in csv_files}

                for future in concurrent.futures.as_completed(future_to_file):
                    success, file_path, error, row_count = future.result()

                    if success:
                        success_count += 1
                        total_rows += row_count
                        logger.debug(f"Successfully processed {file_path}")
                    else:
                        logger.error(f"Failed to process {file_path}: {error}")
                    
                    pbar.update(1)

        logger.success(f"Matching file processing complete: {success_count}/{len(csv_files)} files successfully processed")
        logger.info(f"Total rows processed: {total_rows:,}")

        return success_count, total_rows


def main():
    processor = MatchingFileProcessor()
    processor.run()


if __name__ == "__main__":
    main()
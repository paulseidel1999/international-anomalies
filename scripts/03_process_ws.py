import concurrent.futures
import os
import re
from pathlib import Path

import pandas as pd
import pathos.multiprocessing as mp
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from tqdm import tqdm

WS_FILE_COLUMNS = {
    "WSCalendarPrd":             ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "value"],
    "WSCurrent":                 ["ws_id", "point_date", "item_code", "value"],
    "WSCurrentFootnote":         ["ws_id", "point_date", "item_code", "note"],
    "WSFV":                      ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "value"],
    "WSFVFootnote":              ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "note"],
    "WSIndex":                   ["ws_id", "point_date", "index_name", "item_code", "value"],
    "WSMetaData":                ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "value"],
    "WSMetaDataFootnote":        ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "note"],
    "WSMonthlyPricing":          ["ws_id", "point_date", "item_code", "value"],
    "WSMonthlyPricingFootnote":  ["ws_id", "point_date", "item_code", "note"],
    "WSRatios":                  ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "value"],
    "WSRatiosFootnote":          ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "note"],
    "WSReportedPrd":             ["ws_id", "point_date", "freq", "item_code", "value"],
    "WSSegment":                 ["ws_id", "point_date", "freq", "fiscal_period", "segment_type", "segment_id", "item_code", "value"],
    "WSSegmentFootnote":         ["ws_id", "point_date", "freq", "fiscal_period", "segment_type", "segment_id", "item_code", "note"],
    "WSSupplemental":            ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "value"],
    "WSSupplementalFootnote":    ["ws_id", "point_date", "freq", "fiscal_period", "item_code", "note"],
    "WSWeeklyPricing":           ["ws_id", "point_date", "item_code", "value"],
    "WSWeeklyPricingFootnote":   ["ws_id", "point_date", "item_code", "note"],
}

class WorldscopeProcessor:
    def __init__(self):
        self.root_dir = Path(__file__).resolve().parents[1]
        self.raw_dir = self.root_dir / "data" / "raw"
        self.interim_dir = self.root_dir / "data" / "interim"

        # (1) Adjusted path to "Anomaly Publication/Data/Worldscope"
        self.ws_dir = self.raw_dir / "Anomaly Publication" / "Data" / "Worldscope"

        self.interim_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir = self.interim_dir / "worldscope"
        self.output_dir.mkdir(exist_ok=True)

    def find_txt_files(self):
        txts = list(self.ws_dir.rglob("*.txt"))
        logger.info(f"Found {len(txts)} Worldscope .txt files")
        return txts

    def get_columns(self, filepath):
        file_type = filepath.name.split("_")[0]
        if file_type in WS_FILE_COLUMNS:
            return WS_FILE_COLUMNS[file_type]
        else:
            raise ValueError(f"Unknown Worldscope file type: {file_type}")

    def convert_to_parquet(self, input_file_path, chunk_size=100_000, separator='|'):
        input_file_path = Path(input_file_path)
        input_filename = input_file_path.stem
        output_file = self.output_dir / f"{input_filename}.parquet"

        file_type_match = re.match(r'(WS[a-zA-Z]+)', input_filename)
        if file_type_match and file_type_match.group(1) in WS_FILE_COLUMNS:
            schema = WS_FILE_COLUMNS[file_type_match.group(1)]
        else:
            # fallback: try to read first line as header
            with open(input_file_path, 'r', encoding='windows-1252') as f:
                first_line = f.readline().strip()
                schema = first_line.split(separator)

        writer = None
        total_rows = 0

        try:
            for chunk_df in pd.read_csv(
                input_file_path,
                sep=separator,
                chunksize=chunk_size,
                header=None,
                names=schema,
                dtype_backend='pyarrow',
                encoding='windows-1252'
            ):
                if "point_date" in chunk_df.columns:
                    chunk_df["point_date"] = pd.to_datetime(
                        chunk_df["point_date"], format="%Y%m%d", errors="coerce"
                    )
                for col in ["fiscal_period", "item_code"]:
                    if col in chunk_df.columns:
                        chunk_df[col] = pd.to_numeric(chunk_df[col], errors="coerce")

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
        logger.info("Starting Worldscope .txt processing")

        if not self.ws_dir.exists():
            logger.error(f"Worldscope directory not found: {self.ws_dir}")
            return

        txt_files = self.find_txt_files()
        if not txt_files:
            logger.warning("No .txt files found!")
            return

        # Original: num_processes = mp.cpu_count()
        # We'll still read that, but we'll keep the concurrency lower for threads
        num_processes = mp.cpu_count()
        logger.info(f"Detected {num_processes} CPU cores")

        # (2) Limit the ThreadPool to avoid resource or concurrency issues
        max_threads = min(os.cpu_count(), 4)
        logger.info(f"Using {max_threads} threads for processing")

        success_count = 0
        total_rows = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            with tqdm(total=len(txt_files), desc="Processing WS .txt files") as pbar:
                future_to_file = {executor.submit(self.convert_to_parquet, file): file for file in txt_files}

                for future in concurrent.futures.as_completed(future_to_file):
                    success, _, _, row_count = future.result()

                    if success:
                        success_count += 1
                        total_rows += row_count
                    pbar.update(1)

        logger.success(
            f"Worldscope processing complete: "
            f"{success_count}/{len(txt_files)} files successfully processed"
        )
        logger.info(f"Total rows processed: {total_rows:,}")

        return success_count, total_rows

def main():
    processor = WorldscopeProcessor()
    processor.run()

if __name__ == "__main__":
    main()

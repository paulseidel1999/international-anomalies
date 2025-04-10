import glob
import os
from pathlib import Path

import pandas as pd
from loguru import logger
from tqdm import tqdm

class MatchingFileMerger:
    def __init__(self):
        self.root_dir = Path(__file__).resolve().parents[1]
        self.interim_dir = self.root_dir / "data" / "interim"
        self.matching_dir = self.interim_dir / "universal matching file"
        self.output_file = self.matching_dir / "UniverseMatchingFile_consolidated.parquet"
        self.key_columns = ["DSCD", "WC06105"]

    def _ensure_directory_exists(self):
        self.matching_dir.mkdir(parents=True, exist_ok=True)

    def convert_to_parquet(self, input_folder: Path) -> bool:
        try:
            parquet_files = glob.glob(os.path.join(input_folder, "*.parquet"))
            if not parquet_files:
                logger.warning(f"No parquet files found in {input_folder}")
                return False

            all_data = []
            for file_path in tqdm(parquet_files, desc="Reading parquet files"):
                try:
                    df = pd.read_parquet(file_path)
                    all_data.append(df)
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {str(e)}")

            if not all_data:
                logger.error("No valid data frames to process")
                return False

            logger.info("Combining data frames")
            combined_df = pd.concat(all_data, ignore_index=True)

            logger.info("Processing dates and deduplicating")
            try:
                combined_df['_temp_date'] = pd.to_datetime(
                    combined_df['TIME'],
                    format='%d/%m/%Y',
                    errors='coerce'
                )
                combined_df = combined_df.sort_values(by=['_temp_date'], ascending=False)
                combined_df = combined_df.drop_duplicates(subset=self.key_columns, keep='first')
                combined_df = combined_df.drop(columns=['_temp_date'])
            except Exception as e:
                logger.warning(f"Date processing failed: {str(e)}")
                combined_df = combined_df.drop_duplicates(subset=self.key_columns)

            logger.info(f"Saving consolidated file to {self.output_file}")
            self._ensure_directory_exists()
            combined_df.to_parquet(self.output_file, index=False)
            return True

        except Exception as e:
            logger.error(f"Unexpected error during processing: {str(e)}")
            return False

    def run(self):
        logger.info("Starting Matching file processing")
        success = self.convert_to_parquet(self.matching_dir)
        if success:
            logger.info(f"Successfully created {self.output_file}")
        else:
            logger.error("Processing failed")


def main():
    processor = MatchingFileMerger()
    processor.run()


if __name__ == "__main__":
    main()
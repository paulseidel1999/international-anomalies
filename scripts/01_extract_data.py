import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psutil
from loguru import logger
from tqdm import tqdm

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
ZIP_FILE_PATH = RAW_DATA_DIR / "Anomaly Publication.zip"

logger.remove()
logger.add(
    lambda msg: tqdm.write(msg, end=""),
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

def ensure_directories():
    dirs = [
        RAW_DATA_DIR,
        ROOT_DIR / "data" / "interim",
        ROOT_DIR / "data" / "processed"
    ]
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)

def extract_zip_chunk(args):
    """Used in the original code for chunked multi-thread extractions if desired."""
    zip_path, extract_dir, file_chunk = args
    extracted = 0
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in file_chunk:
                zip_ref.extract(file, extract_dir)
                extracted += 1
        return extracted
    except Exception as e:
        logger.error(f"Error extracting chunk from {zip_path}: {e}")
        return extracted

def extract_single_zip(nested_zip):
    """Extract an entire ZIP file into its parent folder."""
    try:
        extract_dir = nested_zip.parent
        with zipfile.ZipFile(nested_zip, 'r') as zip_ref:
            file_count = len(zip_ref.namelist())
            for file in zip_ref.namelist():
                zip_ref.extract(file, extract_dir)
        return file_count
    except Exception as e:
        logger.error(f"Error extracting {nested_zip}: {e}")
        return 0

def extract_zip():
    if not ZIP_FILE_PATH.exists():
        logger.error(f"Zip file not found: {ZIP_FILE_PATH}")
        return False

    # --------------------------------------------------------------------------
    # STEP 1: SINGLE-THREAD extraction of the main ZIP
    # --------------------------------------------------------------------------
    logger.info(f"Extracting all contents of {ZIP_FILE_PATH.name} (single-thread)...")
    try:
        with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
            zip_ref.extractall(RAW_DATA_DIR)
    except Exception as e:
        logger.error(f"Error extracting main zip {ZIP_FILE_PATH}: {e}")
        return False

    # --------------------------------------------------------------------------
    # STEP 2: FIND NESTED ZIPs (AT ANY DEPTH) INSIDE .../data/raw/
    # --------------------------------------------------------------------------
    nested_zips = list(RAW_DATA_DIR.glob('**/*.ZIP')) + list(RAW_DATA_DIR.glob('**/*.zip'))
    nested_zips = [z for z in nested_zips if z != ZIP_FILE_PATH]

    if not nested_zips:
        logger.info("No nested zip files found")
        return True

    logger.info(f"Found {len(nested_zips)} nested zip files")

    # --------------------------------------------------------------------------
    # STEP 3: SINGLE-THREAD extraction of EACH nested ZIP
    # --------------------------------------------------------------------------
    total_nested_files = 0
    nested_zip_counts = {}
    for zip_path in nested_zips:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_count = len(zip_ref.namelist())
                nested_zip_counts[zip_path] = file_count
                total_nested_files += file_count
        except Exception as e:
            logger.error(f"Error reading {zip_path}: {e}")

    nested_progress = tqdm(total=total_nested_files, desc="Extracting nested archives", unit="files")

    # Instead of concurrency, we just call extract_single_zip() in a loop:
    for zip_path in nested_zips:
        extracted_count = extract_single_zip(zip_path)
        nested_progress.update(extracted_count)

    nested_progress.close()
    return True

def main():
    try:
        ensure_directories()

        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)
        logger.info(
            f"System resources: {os.cpu_count()} CPUs ({cpu_percent}% used), "
            f"Memory: {memory.percent}% used ({memory.available / (1024**3):.1f}GB available)"
        )

        if extract_zip():
            logger.info("Data extraction completed successfully")
        else:
            logger.error("Data extraction failed")

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")

if __name__ == "__main__":
    main()

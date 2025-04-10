import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import psutil
from loguru import logger
from tqdm import tqdm

ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = ROOT_DIR / "data" / "raw"
ZIP_FILE_PATH = RAW_DATA_DIR / "Data.zip"

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

    cpu_count = os.cpu_count() or 1
    io_multiplier = 4 
    max_workers = min(cpu_count * io_multiplier, 32)
    
    # Step 1: Extract main zip with optimized chunking
    logger.info(f"Starting extraction of {ZIP_FILE_PATH.name} with {max_workers} threads")
    
    with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        total_files = len(file_list)
        
        chunk_size = max(1, total_files // max_workers)
        chunks = [file_list[i:i + chunk_size] for i in range(0, total_files, chunk_size)]
    
    main_progress = tqdm(total=total_files, desc=f"Extracting main archive", unit="files")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = [(ZIP_FILE_PATH, RAW_DATA_DIR, chunk) for chunk in chunks]
        
        for extracted_count in executor.map(extract_zip_chunk, tasks):
            main_progress.update(extracted_count)
    
    main_progress.close()

    # Step 2: Find nested zips
    nested_zips = list(RAW_DATA_DIR.glob('**/*.ZIP')) + list(RAW_DATA_DIR.glob('**/*.zip'))
    nested_zips = [z for z in nested_zips if z != ZIP_FILE_PATH]

    if not nested_zips:
        logger.info("No nested zip files found")
        return True

    # Step 3: Extract nested zips
    logger.info(f"Found {len(nested_zips)} nested zip files")
    
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
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_zip = {executor.submit(extract_single_zip, zip_path): zip_path for zip_path in nested_zips}
        
        for future in as_completed(future_to_zip):
            extracted_count = future.result()
            nested_progress.update(extracted_count)
    
    nested_progress.close()
    
    return True

def main():
    try:
        ensure_directories()
        
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=0.1)
        logger.info(f"System resources: {os.cpu_count()} CPUs ({cpu_percent}% used), "
                   f"Memory: {memory.percent}% used ({memory.available / (1024**3):.1f}GB available)")
        
        if extract_zip():
            logger.info("Data extraction completed successfully")
        else:
            logger.error("Data extraction failed")
            
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")

if __name__ == "__main__":
    main()
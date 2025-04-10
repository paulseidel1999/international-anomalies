import os
import sys
import shutil
import argparse
from pathlib import Path
from loguru import logger

logger.remove() 
logger.add(sys.stderr, level="INFO") 

def get_dir_size(path):
    total_size = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total_size += entry.stat().st_size
            elif entry.is_dir():
                total_size += get_dir_size(entry.path)
    return total_size

def format_size(size_bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024 or unit == "TB":
            if unit == "B":
                return f"{size_bytes} {unit}"
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024

def cleanup_directories(directories, dry_run=False):
    total_freed = 0
    
    for directory, description in directories:
        directory = Path(directory)
        if not directory.exists():
            logger.info(f"{description} ({directory}) does not exist. Skipping.")
            continue
        
        dir_size = get_dir_size(directory)
        total_freed += dir_size
        
        if dry_run:
            logger.info(f"Would remove {description} ({directory}): {format_size(dir_size)}")
        else:
            logger.info(f"Removing {description} ({directory}): {format_size(dir_size)}")
            try:
                shutil.rmtree(directory)
                logger.success(f"Successfully removed {directory}")
            except Exception as e:
                logger.error(f"Failed to remove {directory}: {str(e)}")
                total_freed -= dir_size
    
    logger.info(f"Total space {'that would be' if dry_run else ''} freed: {format_size(total_freed)}")
    return total_freed

def selective_cleanup(directory, extensions_to_keep, dry_run=False):
    total_freed = 0
    directory = Path(directory)
    
    if not directory.exists():
        logger.info(f"Directory {directory} does not exist. Skipping.")
        return 0
    
    files_to_remove = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = Path(root) / file
            if not any(file.lower().endswith(ext.lower()) for ext in extensions_to_keep):
                files_to_remove.append(file_path)
                total_freed += file_path.stat().st_size
    
    if files_to_remove:
        if dry_run:
            logger.info(f"Would remove {len(files_to_remove)} files from {directory}, keeping only {', '.join(extensions_to_keep)} files.")
            logger.info(f"Space that would be freed: {format_size(total_freed)}")
        else:
            logger.info(f"Removing {len(files_to_remove)} files from {directory}, keeping only {', '.join(extensions_to_keep)} files.")
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                except Exception as e:
                    logger.error(f"Failed to remove {file_path}: {str(e)}")
                    total_freed -= file_path.stat().st_size
            logger.info(f"Space freed: {format_size(total_freed)}")
    else:
        logger.info(f"No files to remove in {directory}.")
    
    return total_freed

def main():
    parser = argparse.ArgumentParser(description="Clean up intermediate data files")
    parser.add_argument("--dry-run", action="store_true", help="Simulate cleanup without removing files")
    parser.add_argument("--keep-raw", action="store_true", help="Keep raw data files")
    parser.add_argument("--keep-interim", action="store_true", help="Keep interim data files")
    parser.add_argument("--keep-logs", action="store_true", help="Keep log files")
    parser.add_argument("--keep-profiles", action="store_true", help="Keep profiling data")
    parser.add_argument("--keep-essential-raw", action="store_true", help="Keep only zip, csv, and txt files in raw data")
    args = parser.parse_args()
    
    root_dir = Path(__file__).resolve().parent
    directories_to_clean = []
    raw_data_dir = root_dir / "data" / "raw"
    
    if not args.keep_interim:
        directories_to_clean.append((root_dir / "data" / "interim", "Interim data"))
    
    if not args.keep_raw:
        if args.keep_essential_raw:
            pass
        else:
            directories_to_clean.append((raw_data_dir, "Raw data"))
    
    if not args.keep_logs:
        directories_to_clean.append((root_dir / "logs", "Log files"))
    
    if not args.keep_profiles:
        directories_to_clean.append((root_dir / "profiles", "Profiling data"))
    
    if not directories_to_clean and not (not args.keep_raw and args.keep_essential_raw):
        logger.info("No directories selected for cleanup.")
        return
    
    if args.dry_run:
        logger.info("Running in dry-run mode. No files will be removed.")
    
    total_freed = cleanup_directories(directories_to_clean, dry_run=args.dry_run)
    
    if not args.keep_raw and args.keep_essential_raw:
        extensions_to_keep = ['.zip', '.csv', '.txt']
        logger.info(f"Selectively cleaning raw data directory, keeping only {', '.join(extensions_to_keep)} files.")
        freed = selective_cleanup(raw_data_dir, extensions_to_keep, dry_run=args.dry_run)
        total_freed += freed
    
    if not args.dry_run:
        logger.success(f"Cleanup completed successfully. Total space freed: {format_size(total_freed)}")
    else:
        logger.info(f"Dry run completed. Total space that would be freed: {format_size(total_freed)}")

if __name__ == "__main__":
    main()
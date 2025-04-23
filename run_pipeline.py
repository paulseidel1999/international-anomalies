import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime
import importlib.util
from loguru import logger

log_path = Path("logs")
log_path.mkdir(exist_ok=True)
log_file = log_path / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logger.remove()  
logger.add(sys.stderr, level="INFO")  
logger.add(log_file, rotation="100 MB", level="DEBUG") 

def import_script(script_path):
    script_path = Path(script_path)
    module_name = script_path.stem
    
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    return module

def run_step(step_name, script_path):
    logger.info(f"Starting step: {step_name}")
    start_time = time.time()
    
    module = import_script(script_path)
    module.main()
    
    elapsed_time = time.time() - start_time
    logger.success(f"Completed step: {step_name} in {elapsed_time:.2f} seconds")

def main():
    parser = argparse.ArgumentParser(description="Run the financial data processing pipeline")
    parser.add_argument("--steps", nargs="+", type=int, help="Specific steps to run (e.g., --steps 1 3)")
    args = parser.parse_args()
    
    
    pipeline_steps = [

#Extraction process and DS setup
        ("Extract data", "scripts/01_extract_data.py"),
        ("Process Datastream data", "scripts/02_process_ds.py"),
        ("Process Worldscope data", "scripts/03_process_ws.py"),
        ("Process matching files", "scripts/04_process_matching_files.py"),
        ("Merge matching files", "scripts/05_merge_matching_files.py"),
        ("Merge datastream files", "scripts/06_merge_ds_files.py"),
        ("Merge datastream and Matching", "scripts/07_merge_ds_mts.py"),
        #Placeholder: Data clearning DS: Drop missing matching variable, no value
#Prepare WS Data
        #Merge WS Values with PRD Data
        ("Add Period info WS data", "scripts/08_merge_prd_in_WS.py"),        #for FV, Ratios, Suppl. and Current
        #Data cleaning WS: Drop: No PRD data
        ("Drop if missing PRD", "scripts/09_Drop_if_missing_PRD.py"),
        #Drop if data is old 
        ("Drop nonrecent data", "scripts/10_Drop_if_not_recent.py"),
        #Drop unnessary variables
        ("Drop variables, which are not needed ", "scripts/11_Drop_unnes_var.py"),
        #Data clearning DS: Drop missing matching variable, no value

        
#Merge WS into DS        
        ("Dividing the Worldscope dataset", "scripts/12_WS_division.py"), #Adjust such that date information is kept


        
#Quick data analysis 
        ("Generate Table 3 Anomaly Time", "scripts/13_Comparison_PITvsFF92.py"),
        ("Generate Table 3 Anomaly Time", "scripts/14_Comparison_subsample.py"),
        
        ("Compute the return predictors in Worldscope", "scripts/15_compute_anomalies.py"),
        ("Building portfolios based on return predictors FF92", "scripts/16_build_portfolios_ff92.py"),
        
        ("Add Period info WS data", "scripts/20_merge_prd_in_WS.py")       
    ]

#



    
    logger.info(f"Starting data pipeline with")
    
    steps_to_run = args.steps if args.steps else range(1, len(pipeline_steps) + 1)
    
    start_time = time.time()
    
    try:
        for i in steps_to_run:
            if 1 <= i <= len(pipeline_steps):
                step_name, script_path = pipeline_steps[i-1]
                run_step(step_name, script_path)
            else:
                logger.warning(f"Step {i} does not exist. Skipping.")
    
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        total_time = time.time() - start_time
        logger.info(f"Pipeline completed in {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
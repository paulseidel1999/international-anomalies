## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/LudwigFritsch/international-anomalies.git
   cd financial-data-pipeline
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Upload raw data:
   ```bash
   # Upload the Data.zip file in data/raw
   ```

## Directory Structure

```
financial-data-pipeline/
├── data/
│   ├── raw/             # Raw input data (place zip file here)
│   ├── interim/         # Intermediate processed data
│   └── processed/       # Final processed datasets
├── scripts/             # Individual processing scripts
├── run_pipeline.py      # Main pipeline runner
├── cleanup.py           # Cleanup utility
└── requirements.txt     # Python dependencies
```

## Usage

### Running the Pipeline

Run the entire pipeline:
```bash
python run_pipeline.py
```

Run specific steps:
```bash
python run_pipeline.py --steps 1 2
```

### Cleaning Up

Test what would be removed:
```bash
python cleanup.py --dry-run
```

Remove specific data types while keeping others:
```bash
python cleanup.py --keep-raw
```

Remove all intermediate data:
```bash
python cleanup.py
```

## Pipeline Steps

1. **Extract Data**: Extracts files from the main zip archive
2. **Process Datastream**: Processes daily Datastream CSV files into Parquet format
3. **Process Worldscope**: Processes Worlscope TXT files into Parquet format
4. **Process Matching Files**: Processes Universal Matching CSV files into Parquet format
5. **Merge Matching Files**: Merge Universal Matching Parquet files into one Parquet file
5. **Merge Datastream Files**: Merge Datatream  Parquet files into one Parquet file

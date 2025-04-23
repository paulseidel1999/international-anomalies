# scripts/15_compute_anomalies.py
"""
Compute 28 accounting anomalies automatically by looping over a config dict.
Reads only needed Worldscope item tables, pivots via joins, evaluates each anomaly formula,
and writes a wide anomalies table.
"""
import os
from pathlib import Path
import polars as pl

# Force single-threaded to avoid pool issues
os.environ["RAYON_NUM_THREADS"] = "1"
os.environ["POLARS_MAX_THREADS"] = "1"

# ----------------------------------------------------------------------------
# 1) Define anomaly configurations: mapping name -> inputs + formula
# ----------------------------------------------------------------------------
COLUMN_MAP = {
    5490: 'BE',    # Book Equity
    2003: 'Cash',  # Cash
    1051: 'COGS',  # COGS
    2201: 'CA',    # Current Assets
    3101: 'CL',    # Current Liabilities
    9502: 'DivP',  # Dividend Payout
    5255: 'EPS',   # Earnings per Share
    8698: 'SGx',   # Sales Growth - Expected Growth
    2649: 'Intan', # Intangible Assets
    2101: 'Inv',   # Inventory
    18199: 'NDebt',# Net Debt
    6895: 'NI',    # Net Income
    6620: 'PPE',   # Property, Plant & Equipment
    1401: 'PreTax',# Pre-Tax Income
    5006: 'Price', # Market Price
    7240: 'Sales', # Net Sales
    1101: 'SGA',   # SG&A
    3051: 'STD',   # Short-Term Debt
    1451: 'Tax',   # Tax Expense
    6699: 'TA',    # Total Assets
    3351: 'TL',    # Total Liabilities
}

ANOMALIES = {
    # ... definitions as before ...
}

# ----------------------------------------------------------------------------
# 2) Paths
# ----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
WS_DIR = ROOT / 'data' / 'interim' / 'Worldscope_clean_items'
OUTPUT_PATH = ROOT / 'data' / 'processed' / 'anomalies_worldscope.parquet'

# ----------------------------------------------------------------------------
# 3) Main
# ----------------------------------------------------------------------------
def main():
    # gather available item files
    all_codes = sorted({c for cfg in ANOMALIES.values() for c in cfg['inputs']})
    available = []
    for code in all_codes:
        path = WS_DIR / f'WS_item_{code}.parquet'
        col = COLUMN_MAP.get(code, f'item_{code}')
        if path.exists():
            available.append((code, col, path))
        else:
            print(f"⚠️ missing WS_item_{code}.parquet, skipping inputs for code {code}")

    if not available:
        # no individual WS_item_*.parquet files found: fallback to wide merged file
        wide_file = ROOT / 'data' / 'interim' / 'Worldscope_clean' / 'WSFV_merged_20250131_final_no_cols.parquet'
        if not wide_file.exists():
            raise RuntimeError("No WS input files found and wide merged file missing.")
        print(f"⚠️  No WS item files; falling back to wide file: {wide_file}")
        lf = pl.scan_parquet(wide_file)
        # assume wide contains columns named after item_codes: 'cal1_<code>'
        # map wide columns to short names
        for code, col in COLUMN_MAP.items():
            wide_col = f"cal1_{code}"
            if wide_col in lf.columns:
                lf = lf.with_columns(pl.col(wide_col).cast(pl.Float64).alias(col))
        # parse dates and FF92 on wide
        lf = lf.with_columns([
            pl.col('cal1_55350').str.replace_all('"','')
              .str.replace(r'^d','')
              .str.strptime(pl.Date, format='%Y%m%d').alias('fye_date'),
            pl.col('point_date').dt.date().alias('pit_date'),
        ])
        lf = lf.with_columns([
        pl.col('fye_date').dt.offset_by('6mo').alias('ff92_date')
    ])
        df = lf.collect()
        # skip join loop entirely
        available = []  # prevent re-entry
        # end fallback

    # build lazy-frame only if item files exist
    if available:
        lf = None
        for code, col, path in available:
            part = (
                pl.scan_parquet(path)
                  .select([
                      'ws_id', 'point_date', 'freq', 'fiscal_period', 'cal1_55350',
                      # cast and rename the WS value field
                      pl.col('value').str.replace_all('"','').cast(pl.Float64).alias(col)
                  ])
            )
            if lf is None:
                lf = part
            else:
                lf = lf.join(
                    part,
                    on=['ws_id','point_date','freq','fiscal_period','cal1_55350'],
                    how='inner'
                )

        # parse dates and FF92
        lf = lf.with_columns([
            pl.col('cal1_55350')
              .str.replace_all('"','')
              .str.replace(r'^d','')
              .str.strptime(pl.Date, format='%Y%m%d')
              .alias('fye_date'),
            pl.col('point_date').dt.date().alias('pit_date'),
        ])
        lf = lf.with_columns([
            pl.col('fye_date').dt.offset_by('6mo').alias('ff92_date')
        ])

        # collect from lazy frame
        df = lf.collect()
    # if no item files, df is already defined via fallback


    # evaluate anomalies
    for name, cfg in ANOMALIES.items():
        cols = [COLUMN_MAP.get(c, f'item_{c}') for c in cfg['inputs']]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            print(f"⚠️ skip {name}, missing columns {missing}")
            continue
        df = df.with_columns(
            pl.expr.python.eval_expr(
                cfg['formula'],
                local_dict={col: df[col] for col in df.columns}
            ).alias(name)
        )

    # write
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    df.write_parquet(OUTPUT_PATH)
    print(f"Wrote anomalies: {', '.join([n for n in ANOMALIES if n in df.columns])}")

if __name__ == '__main__':
    main()

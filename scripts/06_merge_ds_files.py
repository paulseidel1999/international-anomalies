import os
from concurrent.futures import ThreadPoolExecutor

import pyarrow.parquet as pq
from tqdm import tqdm


class ParquetConsolidator:
    def __init__(self, input_folder, chunk_size=100000, compression='snappy'):
        self.input_folder = input_folder
        self.chunk_size = chunk_size
        self.compression = compression
        self.merged_folder = os.path.join(input_folder, 'merged')
        os.makedirs(self.merged_folder, exist_ok=True)

    def _merge_files(self, mv_file, returns_file, output_file):
        try:
            mv_table = pq.read_table(mv_file)
            returns_table = pq.read_table(returns_file)
            
            merged = returns_table.join(
                mv_table,
                keys=['DSCode', 'Date', 'Currency'],
                join_type="left outer"
            )

            pq.write_table(merged, output_file)
            return output_file, merged.num_rows
        except Exception as e:
            return None, str(e)

    def _append_files(self, input_files, output_file):
        if not input_files:
            raise ValueError("No input files provided")
        
        first_file = pq.ParquetFile(input_files[0])
        schema = first_file.schema_arrow
        
        writer = None
        total_row_groups = sum(pq.ParquetFile(f).num_row_groups for f in input_files)
        
        try:
            with tqdm(total=total_row_groups, desc="Appending files", unit="row group") as pbar:
                for file_path in input_files:
                    parquet_file = pq.ParquetFile(file_path)
                    
                    if writer is None:
                        writer = pq.ParquetWriter(output_file, schema)
                    
                    for row_group_idx in range(parquet_file.num_row_groups):
                        row_group = parquet_file.read_row_group(row_group_idx)
                        writer.write_table(row_group)
                        pbar.update(1)
        finally:
            if writer:
                writer.close()

    def consolidate(self):
        """
        Merges DailyMVUSD with DailyReturnsUSD, and DailyMVLC with DailyReturnsLC.
        The resulting merged datasets are then appended into a single master file.
        """
        print(f"Starting consolidation process for: {self.input_folder}")
        
        files = [f for f in os.listdir(self.input_folder) if f.endswith('.parquet')]
        print(f"Found {len(files)} parquet files")
        
        usd_files = {}
        lc_files = {}
        
        for f in files:
            if 'DailyMVUSD' in f:
                suffix = f.replace('DailyMVUSD', '').replace('.parquet', '')
                usd_files.setdefault(suffix, {})['mv'] = os.path.join(self.input_folder, f)
            elif 'DailyReturnsUSD' in f:
                suffix = f.replace('DailyReturnsUSD', '').replace('.parquet', '')
                usd_files.setdefault(suffix, {})['returns'] = os.path.join(self.input_folder, f)
            elif 'DailyMVLC' in f:
                suffix = f.replace('DailyMVLC', '').replace('.parquet', '')
                lc_files.setdefault(suffix, {})['mv'] = os.path.join(self.input_folder, f)
            elif 'DailyReturnsLC' in f:
                suffix = f.replace('DailyReturnsLC', '').replace('.parquet', '')
                lc_files.setdefault(suffix, {})['returns'] = os.path.join(self.input_folder, f)

        print("Starting merge process")
        merged_files = []
        merge_tasks = []
        
        for suffix, pair in usd_files.items():
            if 'mv' in pair and 'returns' in pair:
                output_file = os.path.join(self.merged_folder, f'MergedUSD{suffix}.parquet')
                if os.path.exists(output_file):
                    print(f"File exists, skipping: MergedUSD{suffix}.parquet")
                    merged_files.append(output_file)
                else:
                    merge_tasks.append((pair['mv'], pair['returns'], output_file, f'MergedUSD{suffix}.parquet'))
        
        for suffix, pair in lc_files.items():
            if 'mv' in pair and 'returns' in pair:
                output_file = os.path.join(self.merged_folder, f'MergedLC{suffix}.parquet')
                if os.path.exists(output_file):
                    print(f"File exists, skipping: MergedLC{suffix}.parquet")
                    merged_files.append(output_file)
                else:
                    merge_tasks.append((pair['mv'], pair['returns'], output_file, f'MergedLC{suffix}.parquet'))
        
        if merge_tasks:
            max_threads = os.cpu_count()
            with tqdm(total=len(merge_tasks), desc="Merging files", unit="file") as pbar:
                with ThreadPoolExecutor(max_workers=max_threads) as executor:
                    futures = []
                    for mv_file, returns_file, output_file, file_name in merge_tasks:
                        futures.append(executor.submit(self._merge_files, mv_file, returns_file, output_file))
                    
                    for future in futures:
                        output_file, result = future.result()
                        if output_file:
                            merged_files.append(output_file)
                            pbar.set_postfix({"Last merged": os.path.basename(output_file), "Rows": result})
                        else:
                            pbar.set_postfix({"Error": result})
                        pbar.update(1)

        final_output = os.path.join(self.input_folder, 'Datastream_consolidated.parquet')
        if os.path.exists(final_output):
            print(f"Final output {final_output} already exists, skipping append")
        elif merged_files:
            print(f"Appending {len(merged_files)} files to create final output")
            self._append_files(merged_files, final_output)
        else:
            print("No merged files to append")
        
        print("Consolidation completed successfully")


def main():
    input_folder = './data/interim/datastream'
    consolidator = ParquetConsolidator(input_folder, chunk_size=100000, compression='snappy')
    consolidator.consolidate()

if __name__ == "__main__":
    main()
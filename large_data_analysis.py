import numpy as np
import pandas as pd
import polars as pl
import dask.dataframe as dd
import modin.pandas as mpd
import duckdb
import time

filename = "data/large_data.parquet"

# Create larger dataset for meaningful performance comparison
def create_large_dataset(n_rows=1_000_000):
    np.random.seed(42)
    large_data = {
        'id': range(n_rows),
        'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], n_rows),
        'value1': np.random.normal(100, 25, n_rows),
        'value2': np.random.normal(50, 15, n_rows),
        'timestamp': pd.date_range('2020-01-01', periods=n_rows, freq='1min'),
        'group_id': np.random.randint(1, 1000, n_rows)
    }
    return large_data

# Save larger dataset
large_data = create_large_dataset()
df_large_pandas = pd.DataFrame(large_data)
df_large_pandas.to_parquet(filename, index=False)

# Load data with different libraries
df_large_polars = pl.read_parquet(filename)
df_large_dask = dd.read_parquet(filename)
df_large_modin = mpd.read_parquet(filename)


# Complex aggregation benchmark
def benchmark_complex_agg():
    times = {}

    # Pandas
    start = time.time()
    result_pandas = (df_large_pandas
    .groupby(['category', df_large_pandas['timestamp'].dt.hour])
    .agg({
        'value1': ['mean', 'std', 'count'],
        'value2': ['sum', 'min', 'max']
    }))
    times['pandas'] = time.time() - start

    # Polars
    start = time.time()
    result_polars = (df_large_polars
    .group_by(['category', pl.col('timestamp').dt.hour()])
    .agg([
        pl.col('value1').mean().alias('value1_mean'),
        pl.col('value1').std().alias('value1_std'),
        pl.col('value1').count().alias('value1_count'),
        pl.col('value2').sum().alias('value2_sum'),
        pl.col('value2').min().alias('value2_min'),
        pl.col('value2').max().alias('value2_max')
    ]))
    times['polars'] = time.time() - start

    # DuckDB
    conn = duckdb.connect()
    start = time.time()
    result_duckdb = conn.execute("""
        SELECT 
            category,
            EXTRACT(hour FROM timestamp) as hour,
            AVG(value1) as value1_mean,
            STDDEV(value1) as value1_std,
            COUNT(value1) as value1_count,
            SUM(value2) as value2_sum,
            MIN(value2) as value2_min,
            MAX(value2) as value2_max
        FROM df_large_pandas
        GROUP BY category, hour
    """).df()
    times['duckdb'] = time.time() - start

    # Modin
    start = time.time()
    result_modin = (df_large_modin
    .groupby(['category', df_large_modin['timestamp'].dt.hour])
    .agg({
        'value1': ['mean', 'std', 'count'],
        'value2': ['sum', 'min', 'max']
    }))
    times['modin'] = time.time() - start

    # Dask
    start = time.time()
    result_dask = (df_large_dask
                   .groupby(['category', df_large_dask['timestamp'].dt.hour])
                   .agg({
        'value1': ['mean', 'std', 'count'],
        'value2': ['sum', 'min', 'max']
    }).compute())
    times['dask'] = time.time() - start

    return times


benchmark_times = benchmark_complex_agg()
print("\n=== LARGE DATASET BENCHMARK (1M rows) ===")
for lib, time_taken in benchmark_times.items():
    print(f"{lib.capitalize()}: {time_taken:.4f}s")
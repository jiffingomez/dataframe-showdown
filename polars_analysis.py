import polars as pl
import time

def load_data(filename):
    # Polars - Fast and memory efficient
    df_polars = pl.read_csv(filename)
    print("Polars DataFrame:")
    print(df_polars.head())
    print(f"Shape: {df_polars.shape}")
    print(f"Memory usage: {df_polars.estimated_size('mb'):.2f} MB")
    return df_polars

def filter_data(df):
    start = time.time()
    result_polars = df.filter(
        (pl.col('active') == True) &
        (pl.col('department') == 'IT') &
        (pl.col('salary') > 60000)
    )
    polars_time = time.time() - start
    print(f"Polars result: {len(result_polars)} rows in {polars_time:.4f}s")

def agg_data(df):
    start = time.time()
    agg_polars = (df
                  .filter(pl.col('active') == True)
                  .group_by('department')
                  .agg([
        pl.col('salary').mean().alias('mean_salary'),
        pl.col('salary').count().alias('count'),
        pl.col('salary').std().alias('std_salary')
    ])
                  .sort('department'))
    polars_agg_time = time.time() - start
    # print("Polars aggregation:")
    # print(agg_polars)
    print(f"Polars aggregation Time: {polars_agg_time:.4f}s")
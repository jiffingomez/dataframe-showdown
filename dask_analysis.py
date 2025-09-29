# Dask - Distributed computing
import dask.dataframe as dd
import time

def load_data(filename):
    df_dask = dd.read_csv(filename)
    print("Dask DataFrame (lazy):")
    print(df_dask.head())  # This triggers computation
    print(f"Shape: {df_dask.shape}")  # This also triggers computation
    return df_dask

def filter_data(df):
    start = time.time()
    result_dask = df[
        (df['active'] == True) &
        (df['department'] == 'IT') &
        (df['salary'] > 60000)
        ].compute()  # .compute() triggers actual computation
    dask_time = time.time() - start
    print(f"Dask result: {len(result_dask)} rows in {dask_time:.4f}s")

def agg_data(df):
    start = time.time()
    agg_dask = (df[df['active'] == True]
                .groupby('department')['salary']
                .agg(['mean', 'count', 'std'])
                .compute()
                .round(2))
    dask_agg_time = time.time() - start
    # print("Dask aggregation:")
    # print(agg_dask)
    print(f"Dask aggregation Time: {dask_agg_time:.4f}s")
import pandas as pd
import time

def load_data(filename):
    # Pandas - Traditional approach
    df_pandas = pd.read_csv(filename)
    print("Pandas DataFrame:")
    print(df_pandas.head())
    print(f"Shape: {df_pandas.shape}")
    print(f"Memory usage: {df_pandas.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    return df_pandas

def filter_data(df):
    start = time.time()
    result_pandas = df[
        (df['active'] == True) &
        (df['department'] == 'IT') &
        (df['salary'] > 60000)
        ]
    pandas_time = time.time() - start
    print(f"Pandas result: {len(result_pandas)} rows in {pandas_time:.4f}s")

def agg_data(df):
    start = time.time()
    agg_pandas = (df[df['active'] == True]
                  .groupby('department')['salary']
                  .agg(['mean', 'count', 'std'])
                  .round(2))
    pandas_agg_time = time.time() - start
    # print("Pandas aggregation:")
    # print(agg_pandas)
    print(f"Pandas aggregation Time: {pandas_agg_time:.4f}s")
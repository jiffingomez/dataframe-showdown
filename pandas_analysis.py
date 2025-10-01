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

def complex_transformation(df):
    start = time.time()
    transform_pandas = df.copy()
    transform_pandas['age_group'] = pd.cut(
        transform_pandas['age'],
        bins=[0, 30, 50, 100],
        labels=['Young', 'Middle', 'Senior']
    )
    transform_pandas['salary_percentile'] = (
            transform_pandas.groupby('department')['salary']
            .rank(pct=True) * 100
    ).round(2)
    pandas_transform_time = time.time() - start
    print(f"Pandas transformation time: {pandas_transform_time:.4f}s")
    # print(transform_pandas[['age', 'age_group', 'salary', 'salary_percentile']].head())

def join_analysis(df, df_1):
    start = time.time()
    joined_pandas = df.merge(df_1, on='department', how='left')
    pandas_join_time = time.time() - start
    print(f"Pandas join time: {pandas_join_time:.4f}s")
    # print(f"Joined shape: {joined_pandas.shape}")
    # print(joined_pandas[['name', 'department', 'salary', 'budget', 'manager']].head())

def window_fn(df):
    start = time.time()
    window_pandas = (df.sort_values(['department', 'salary'])
                     .assign(running_total=lambda x: x.groupby('department')['salary'].cumsum()))
    pandas_window_time = time.time() - start
    print(f"Pandas window time: {pandas_window_time:.4f}s")
    # print(window_pandas[['name', 'department', 'salary', 'running_total']].head(10))
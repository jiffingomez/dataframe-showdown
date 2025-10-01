# Dask - Distributed computing
import dask.dataframe as dd
import pandas as pd
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

def complex_transformation(df):
    start = time.time()
    # Dask approach - note: cut requires compute for bins
    transform_dask = df.copy()
    transform_dask['age_group'] = dd.from_pandas(
        pd.cut(df['age'].compute(), bins=[0, 30, 50, 100], labels=['Young', 'Middle', 'Senior']),
        npartitions=df.npartitions
    )
    # For percentile ranking, we need to compute
    temp_df = transform_dask.compute()
    temp_df['salary_percentile'] = (
            temp_df.groupby('department')['salary']
            .rank(pct=True) * 100
    ).round(2)
    transform_dask = dd.from_pandas(temp_df, npartitions=df.npartitions)
    dask_transform_time = time.time() - start
    print(f"Dask transformation time: {dask_transform_time:.4f}s")
    # print(transform_dask[['age', 'age_group', 'salary', 'salary_percentile']].head())

def join_analysis(df, df_1):
    start = time.time()
    # Convert budget DataFrame to Dask DataFrame
    budget_df_dask = dd.from_pandas(df_1, npartitions=1)
    # Perform the merge and compute
    joined_dask = df.merge(budget_df_dask, on='department', how='left').compute()
    dask_join_time = time.time() - start
    print(f"Dask join time: {dask_join_time:.4f}s")
    # print(f"Joined shape: {joined_dask.shape}")
    # print(joined_dask[['name', 'department', 'salary', 'budget', 'manager']].head())

def lazy_eval(df):
    # Dask also uses lazy evaluation
    start = time.time()
    lazy_dask = (df[df['active'] == True]
                 [df['salary'] > 50000]
                 .groupby('department')
                 .agg({'salary': 'mean', 'age': 'mean'})
                 .compute())  # Only compute when needed
    dask_lazy_time = time.time() - start
    print(f"Dask lazy execution time: {dask_lazy_time:.4f}s")
    # print(lazy_dask)

def window_fn(df):
    start = time.time()
    # Dask window functions require compute for proper partitioning
    df_sorted = df.compute().sort_values(['department', 'salary'])
    df_sorted['running_total'] = df_sorted.groupby('department')['salary'].cumsum()
    window_dask = df_sorted
    dask_window_time = time.time() - start
    print(f"Dask window time: {dask_window_time:.4f}s")
    # print(window_dask[['name', 'department', 'salary', 'running_total']].head(10))
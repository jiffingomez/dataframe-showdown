# Modin - Parallel pandas
import modin.pandas as mpd
import time

def load_data(filename):
    df_modin = mpd.read_csv(filename)
    print("Modin DataFrame:")
    print(df_modin.head())
    print(f"Shape: {df_modin.shape}")
    return df_modin

def filter_data(df):
    start = time.time()
    result_modin = df[
        (df['active'] == True) &
        (df['department'] == 'IT') &
        (df['salary'] > 60000)
        ]
    modin_time = time.time() - start
    print(f"Modin result: {len(result_modin)} rows in {modin_time:.4f}s")

def agg_data(df):
    start = time.time()
    agg_modin = (df[df['active'] == True]
                 .groupby('department')['salary']
                 .agg(['mean', 'count', 'std'])
                 .round(2))
    modin_agg_time = time.time() - start
    # print("Modin aggregation:")
    # print(agg_modin)
    print(f"Modin aggregation Time: {modin_agg_time:.4f}s")

def complex_transformation(df):
    start = time.time()
    transform_modin = df.copy()
    transform_modin['age_group'] = mpd.cut(
        transform_modin['age'],
        bins=[0, 30, 50, 100],
        labels=['Young', 'Middle', 'Senior']
    )
    transform_modin['salary_percentile'] = (
            transform_modin.groupby('department')['salary']
            .rank(pct=True) * 100
    ).round(2)
    modin_transform_time = time.time() - start
    print(f"Modin transformation time: {modin_transform_time:.4f}s")
    # print(transform_modin[['age', 'age_group', 'salary', 'salary_percentile']].head())

def join_analysis(df, df_1):
    start = time.time()
    joined_modin = df.merge(df_1, on='department', how='left')
    modin_join_time = time.time() - start
    print(f"Modin join time: {modin_join_time:.4f}s")
    # print(f"Joined shape: {joined_modin.shape}")
    # print(joined_modin[['name', 'department', 'salary', 'budget', 'manager']].head())

def window_fn(df):
    start = time.time()
    window_modin = (df.sort_values(['department', 'salary'])
                    .assign(running_total=lambda x: x.groupby('department')['salary'].cumsum()))
    modin_window_time = time.time() - start
    print(f"Modin window time: {modin_window_time:.4f}s")
    # print(window_modin[['name', 'department', 'salary', 'running_total']].head(10))
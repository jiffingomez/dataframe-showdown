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

def complex_transformation(df):
    start = time.time()
    transform_polars = df.with_columns([
        pl.col('age').cut([30, 50], labels=['Young', 'Middle', 'Senior']).alias('age_group'),
        (pl.col('salary').rank('dense').over('department') /
         pl.col('salary').count().over('department') * 100).round(2).alias('salary_percentile')
    ])
    polars_transform_time = time.time() - start
    print(f"Polars transformation time: {polars_transform_time:.4f}s")
    # print(transform_polars.select(['age', 'age_group', 'salary', 'salary_percentile']).head())

def join_analysis(df, df_1):
    start = time.time()
    joined_polars = df.join(df_1, on='department', how='left')
    polars_join_time = time.time() - start
    print(f"Polars join time: {polars_join_time:.4f}s")
    # print(f"Joined shape: {joined_polars.shape}")
    # print(joined_polars.select(['name', 'department', 'salary', 'budget', 'manager']).head())

def lazy_eval(filename):
    # Polars lazy evaluation - builds query plan without execution
    lazy_query = (pl.scan_csv(filename)
                  .filter(pl.col('active') == True)
                  .filter(pl.col('salary') > 50000)
                  .group_by('department')
                  .agg([
        pl.col('salary').mean().alias('avg_salary'),
        pl.col('age').mean().alias('avg_age')
    ])
                  .sort('avg_salary', descending=True))

    # print("Polars Query plan:")
    # print(lazy_query.explain())

    # Execute the query
    start = time.time()
    result_polars_lazy = lazy_query.collect()
    polars_lazy_time = time.time() - start
    print(f"Polars lazy execution time: {polars_lazy_time:.4f}s")
    # print(result_polars_lazy)

def window_fn(df):
    start = time.time()
    window_polars = (df
    .sort(['department', 'salary'])
    .with_columns(
        pl.col('salary').cum_sum().over('department').alias('running_total')
    ))
    polars_window_time = time.time() - start
    print(f"Polars window time: {polars_window_time:.4f}s")
    # print(window_polars.select(['name', 'department', 'salary', 'running_total']).head(10))
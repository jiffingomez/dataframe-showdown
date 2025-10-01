"""
This file orchestrates the analysis
"""
import pandas as pd
import polars as pl
import modin.pandas as mpd
import dask_analysis
import duckdb_analysis
import modin_analysis
import polars_analysis
import sample_data
import pandas_analysis

# generate sample data
filename = 'data/employee.csv'
sample_data.generate(filename)

"""
Scenario 1: Basic Data Loading and Inspection
"""
print("Basic Data Loading and Inspection")

print("\nPandas Analysis: ")
pd_df = pandas_analysis.load_data(filename)

print("\nPolars Analysis: ")
pl_df = polars_analysis.load_data(filename)

print("\nDuckDB Analysis: ")
conn, ddb_df = duckdb_analysis.load_data(filename)

print("\nModin Analysis: ")
md_df = modin_analysis.load_data(filename)

print("\nDask Analysis: ")
da_df = dask_analysis.load_data(filename)
print("\n")

"""
Scenario 2: Data Filtering and Selection
"""
print("Data filtering and selection analysis:")
pandas_analysis.filter_data(pd_df)
polars_analysis.filter_data(pl_df)
duckdb_analysis.filter_data(conn,ddb_df)
modin_analysis.filter_data(md_df)
dask_analysis.filter_data(da_df)
print("\n")

"""
Scenario 3: Aggregations and Group By
"""
print("Aggregations and Group By analysis:")
pandas_analysis.agg_data(pd_df)
polars_analysis.agg_data(pl_df)
duckdb_analysis.agg_data(conn, ddb_df)
modin_analysis.agg_data(md_df)
dask_analysis.agg_data(da_df)
print("\n")

"""
Scenario 4: Complex Transformations
"""
print("Add age groups and calculate salary percentiles")
pandas_analysis.complex_transformation(pd_df)
polars_analysis.complex_transformation(pl_df)
duckdb_analysis.complex_transformation(conn, ddb_df)
modin_analysis.complex_transformation(md_df)
dask_analysis.complex_transformation(da_df)
print("\n")
"""
Scenario 5: Joining Data
"""
# Create additional dataset for joins
# Department budget data
dept_budget = {
    'department': ['IT', 'HR', 'Finance', 'Marketing'],
    'budget': [1000000, 500000, 800000, 600000],
    'manager': ['Alice', 'Bob', 'Charlie', 'Diana']
}
budget_df_pandas = pd.DataFrame(dept_budget)
budget_df_polars = pl.DataFrame(dept_budget)
budget_df_modin = mpd.DataFrame(dept_budget)

print("Join transformation analysis")
pandas_analysis.join_analysis(pd_df, budget_df_pandas)
polars_analysis.join_analysis(pl_df, budget_df_polars)
duckdb_analysis.join_analysis(conn, ddb_df, budget_df_pandas)
modin_analysis.join_analysis(md_df, budget_df_modin)
dask_analysis.join_analysis(da_df, budget_df_pandas)
print("\n")

"""
Scenario 6: Lazy Evaluation (Polars & Dask Feature)
"""
print("Lazy Evaluation (Polars & Dask Feature)")
# Polars Lazy Evaluation
polars_analysis.lazy_eval(filename)
dask_analysis.lazy_eval(da_df)
print("\n")

"""
Scenario 7: Window Functions
Task: Calculate running total of salaries by department
"""
print(f"\n🪟 Window Functions (100K rows):")
pandas_analysis.window_fn(pd_df)
polars_analysis.window_fn(pl_df)
duckdb_analysis.window_fn(conn, ddb_df)
modin_analysis.window_fn(md_df),
dask_analysis.window_fn(da_df)
print("\n")
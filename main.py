"""
This file orchestrates the analysis
"""
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
print("\n")
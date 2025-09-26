import duckdb
import time

def load_data(filename):
    # DuckDB - SQL interface
    conn = duckdb.connect()
    df_duckdb = conn.execute(f"SELECT * FROM '{filename}'").df()
    print("DuckDB DataFrame:")
    print(df_duckdb.head())
    print(f"Shape: {df_duckdb.shape}")
    return conn, df_duckdb

def filter_data(conn, df):
    start = time.time()
    result_duckdb = conn.execute("""
        SELECT * FROM df
        WHERE active = true 
        AND department = 'IT' 
        AND salary > 60000
    """).df()
    duckdb_time = time.time() - start
    print(f"DuckDB result: {len(result_duckdb)} rows in {duckdb_time:.4f}s")

def agg_data(conn, df):
    start = time.time()
    agg_duckdb = conn.execute("""
        SELECT 
            department,
            ROUND(AVG(salary), 2) as mean_salary,
            COUNT(*) as count,
            ROUND(STDDEV(salary), 2) as std_salary
        FROM df
        WHERE active = true
        GROUP BY department
        ORDER BY department
    """).df()
    duckdb_agg_time = time.time() - start
    # print("DuckDB aggregation:")
    # print(agg_duckdb)
    print(f"DuckDB aggregation: Time: {duckdb_agg_time:.4f}s")
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

def complex_transformation(conn, df):
    start = time.time()
    conn.execute("DROP TABLE IF EXISTS employees_transformed")
    conn.execute("""
        CREATE TABLE employees_transformed AS
        SELECT *,
            CASE 
                WHEN age <= 30 THEN 'Young'
                WHEN age <= 50 THEN 'Middle'
                ELSE 'Senior'
            END as age_group,
            ROUND(
                100.0 * (RANK() OVER (PARTITION BY department ORDER BY salary) - 1) / 
                (COUNT(*) OVER (PARTITION BY department) - 1), 2
            ) as salary_percentile
        FROM df
    """)
    transform_duckdb = conn.execute("""
        SELECT age, age_group, salary, salary_percentile 
        FROM employees_transformed 
        LIMIT 5
    """).df()
    duckdb_transform_time = time.time() - start
    print(f"DuckDB transformation time: {duckdb_transform_time:.4f}s")
    # print(transform_duckdb)

def join_analysis(conn, df, df_1):
    start = time.time()
    joined_duckdb = conn.execute("""
        SELECT e.*, b.budget, b.manager
        FROM df e
        LEFT JOIN df_1 b ON e.department = b.department
    """).df()
    duckdb_join_time = time.time() - start
    print(f"DuckDB join time: {duckdb_join_time:.4f}s")
    # print(f"Joined shape: {joined_duckdb.shape}")
    # print(joined_duckdb[['name', 'department', 'salary', 'budget', 'manager']].head())

def window_fn(conn, df):
    start = time.time()
    window_duckdb = conn.execute("""
        SELECT name, department, salary,
               SUM(salary) OVER (PARTITION BY department ORDER BY salary) as running_total
        FROM df
        ORDER BY department, salary
        LIMIT 10
    """).df()
    duckdb_window_time = time.time() - start
    print(f"DuckDB window time: {duckdb_window_time:.4f}s")
    # print(window_duckdb)
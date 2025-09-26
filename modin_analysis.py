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

# def agg_data(df):

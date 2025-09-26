import numpy as np
from datetime import datetime, timedelta

def generate(filename):
    # Create sample data
    np.random.seed(42)
    n_rows = 100000

    data = {
        'id': range(n_rows),
        'name': [f'User_{i}' for i in range(n_rows)],
        'age': np.random.randint(18, 80, n_rows),
        'salary': np.random.normal(50000, 15000, n_rows),
        'department': np.random.choice(['IT', 'HR', 'Finance', 'Marketing'], n_rows),
        'hire_date': [datetime(2020, 1, 1) + timedelta(days=int(x))
                      for x in np.random.randint(0, 1000, n_rows)],
        'active': np.random.choice([True, False], n_rows, p=[0.8, 0.2])
    }

    # Save to CSV for loading examples
    import csv
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data.keys())
        for i in range(n_rows):
            writer.writerow([data[key][i] for key in data.keys()])

    print(f"Generated sample data: {filename}")
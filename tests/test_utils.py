import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import random

def create_test_files(output_dir: Path = Path(".")):
    """
    Create random CSV and Parquet files with id, name (country), and date fields.
    
    Args:
        output_dir (Path): Directory where files will be saved
    """
    # List of random country names
    countries = [
        "United States", "Canada", "Mexico", "Brazil", "Argentina", "Chile",
        "United Kingdom", "France", "Germany", "Italy", "Spain", "Netherlands",
        "Sweden", "Norway", "Denmark", "Finland", "Poland", "Czech Republic",
        "Australia", "New Zealand", "Japan", "South Korea", "China", "India",
        "Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia", "Singapore",
        "Egypt", "South Africa", "Kenya", "Nigeria", "Morocco", "Ghana",
        "Russia", "Turkey", "Greece", "Portugal", "Switzerland", "Austria",
        "Belgium", "Ireland", "Iceland", "Luxembourg", "Estonia", "Latvia",
        "Lithuania", "Slovenia", "Croatia", "Serbia", "Bulgaria", "Romania"
    ]
    
    # Create data
    data = []
    dates = ["2025-04-01", "2020-04-05", "2020-09-25"]
    
    # Generate 10 rows for each date (30 total)
    for i, date in enumerate(dates):
        for j in range(10):
            row_id = i * 10 + j + 1
            country = random.choice(countries)
            data.append({
                "id": row_id,
                "name": country,
                "date": date
            })
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Shuffle the rows to make it more realistic
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save as CSV
    csv_path = output_dir / "test_data.csv"
    df.to_csv(csv_path, index=False)
    print(f"CSV file created: {csv_path}")
    
    # Save as Parquet
    parquet_path = output_dir / "test_data.parquet"
    df.to_parquet(parquet_path, engine='pyarrow', index=False)
    print(f"Parquet file created: {parquet_path}")

    
    return df

# Example usage:
if __name__ == "__main__":
    # Create files in current directory
    output_dir = Path("tests/data/tables")
    create_test_files(output_dir)
#!/usr/bin/env python3
"""
Sample Test Data Generator for GCP Data Ingestion Pipeline
Generates valid_batch.csv, corrupt_batch.csv, and mixed_batch.csv
"""

import os
import random
from datetime import datetime, timedelta

SAMPLE_DIR = os.path.dirname(os.path.abspath(__file__))

CATEGORIES = ["ELECTRONICS", "CLOTHING", "GROCERY", "HOME", "AUTOMOTIVE"]


def generate_valid_row(i):
    tx_id = f"TXN-{100000 + i}"
    ts = (datetime.now() - timedelta(minutes=random.randint(1, 1440))).strftime("%Y-%m-%d %H:%M:%S")
    cat = random.choice(CATEGORIES)
    amt = round(random.uniform(5.00, 1500.00), 2)
    return f"{tx_id},{ts},{cat},{amt}\n"


def generate_corrupt_row(i):
    error_type = random.choice(["missing_id", "bad_date", "negative_amt", "truncated"])
    ts = (datetime.now() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
    cat = random.choice(CATEGORIES)

    if error_type == "missing_id":
        return f",{ts},{cat},100.50\n"
    elif error_type == "bad_date":
        return f"TXN-ERR-{i},invalid-date-format,{cat},250.00\n"
    elif error_type == "negative_amt":
        return f"TXN-ERR-{i},{ts},{cat},-99.99\n"
    else:  # truncated column
        return f"TXN-ERR-{i},{ts}\n"


def create_sample_files():
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    # 1. Valid Batch File (100 rows)
    valid_file = os.path.join(SAMPLE_DIR, "valid_batch.csv")
    with open(valid_file, "w") as f:
        f.write("id,timestamp,category,amount\n")
        for i in range(1, 101):
            f.write(generate_valid_row(i))
    print(f"Created {valid_file} (100 valid rows)")

    # 2. Corrupt Batch File (50 corrupt rows)
    corrupt_file = os.path.join(SAMPLE_DIR, "corrupt_batch.csv")
    with open(corrupt_file, "w") as f:
        f.write("id,timestamp,category,amount\n")
        for i in range(1, 51):
            f.write(generate_corrupt_row(i))
    print(f"Created {corrupt_file} (50 corrupt rows)")

    # 3. Mixed Batch File (80 valid rows + 20 corrupt rows)
    mixed_file = os.path.join(SAMPLE_DIR, "mixed_batch.csv")
    with open(mixed_file, "w") as f:
        f.write("id,timestamp,category,amount\n")
        for i in range(1, 81):
            f.write(generate_valid_row(i))
        for i in range(81, 101):
            f.write(generate_corrupt_row(i))
    print(f"Created {mixed_file} (80 valid + 20 corrupt rows)")


if __name__ == "__main__":
    create_sample_files()

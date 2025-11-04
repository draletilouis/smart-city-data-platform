import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime
import glob

RAW_DIR = "data/raw/postgres/"
STAGING_DIR = "data/staging/postgres/"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

POSTGRES_URI = "postgresql://admin:admin@localhost:5432/smartcity"
WAREHOUSE_URI = "postgresql://admin:admin@localhost:5432/smartcity_warehouse"


def extract_postgres_tables(batch_size=1000):
    engine = create_engine(POSTGRES_URI)
    query = "SELECT * FROM ticket_sales;"

    for i, chunk in enumerate(pd.read_sql(query, engine, chunksize=batch_size)):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(RAW_DIR, f"ticket_sales_batch_{i+1}.csv")
        chunk.to_csv(file_path, index=False)
        print(f"✅ Extracted batch {i+1} to {file_path}")


def clean_and_stage():
    all_files = glob.glob(f"{RAW_DIR}*.csv")
    df = pd.concat([pd.read_csv(f) for f in all_files])

    # Cleaning
    df['fare'] = pd.to_numeric(df['fare'], errors='coerce')
    if 'timestamp' not in df.columns:
        df['timestamp'] = pd.Timestamp.now()  # add timestamp if missing
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.drop_duplicates()
    df = df.fillna({'fare': 0})

    # Save staged file
    staged_file = f"{STAGING_DIR}ticket_sales_staged.csv"
    df.to_csv(staged_file, index=False)
    print(f"Saved staged data to {staged_file}")


def load_to_warehouse():
    staged_file = f"{STAGING_DIR}ticket_sales_staged.csv"
    df = pd.read_csv(staged_file)

    engine = create_engine(WAREHOUSE_URI)
    df.to_sql("ticket_sales", engine, if_exists="append", index=False)
    print(f" Loaded staged data into warehouse from {staged_file}")


# ----------------------------
# MASTER FUNCTION
# ----------------------------
def run_postgres_pipeline():
    extract_postgres_tables()
    clean_and_stage()
    load_to_warehouse()
    print("Postgres ingestion pipeline complete!")


if __name__ == "__main__":
    run_postgres_pipeline()

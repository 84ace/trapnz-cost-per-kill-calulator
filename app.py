import os
import io
import pandas as pd
from sqlalchemy import create_engine, text
import time
import psycopg2
from urllib.parse import urlparse
import gzip
import hashlib
import multiprocessing
import sqlalchemy.exc
import os
from sqlalchemy import event

DATABASE_URL = "postgresql://trapuser:trappass@db:5432/trapdb"
OFFLINE_DIR = "./offline"

if not os.path.exists(OFFLINE_DIR):
    os.makedirs(OFFLINE_DIR)

def wait_for_db(url, timeout=30):
    """Wait for the Postgres DB to be ready before continuing."""
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or 5432
    user = parsed.username
    password = parsed.password
    dbname = parsed.path.lstrip('/')

    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
            conn.close()
            print("Database is ready!")
            break
        except psycopg2.OperationalError:
            if time.time() - start > timeout:
                raise Exception("Timed out waiting for database")
            print("Waiting for database...")
            time.sleep(1)

wait_for_db(DATABASE_URL)

engine = create_engine(DATABASE_URL)

@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    connection_record.info["pid"] = os.getpid()

@event.listens_for(engine, "checkout")
def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info["pid"] != pid:
        raise sqlalchemy.exc.DisconnectionError(
            "Connection record belongs to pid %s, "
            "attempting to check out in pid %s" %
            (connection_record.info["pid"], pid)
        )

# Create tables if not exist
with engine.connect() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS processed_files (
        file_id TEXT PRIMARY KEY,
        file_name TEXT,
        processed_at TIMESTAMP DEFAULT NOW()
    );
    """))
    conn.commit()
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS trap_data (
        project_uuid UUID,
        report_uuid UUID,
        report_type TEXT,
        date_start DATE,
        date_end DATE,
        device_device_uuid UUID,
        device_x_device_model_name TEXT,
        device_device_status_arrival TEXT,
        device_device_status_condition TEXT,
        device_device_status_strikes INTEGER,
        location_location_uuid UUID,
        location_location_wkt TEXT,
        substance JSONB,
        species_species_uuid UUID,
        species_x_species_name TEXT,
        species_species_sex TEXT,
        species_species_age TEXT,
        person_uuid UUID
    );
    """))
    conn.commit()

def list_local_files(directory):
    """Lists .csv.gz files in the specified directory."""
    files = []
    for f in os.listdir(directory):
        if f.lower().endswith(".csv.gz"):
            files.append(f)
    return files

def mark_file_processed(file_id, file_name):
    with engine.connect() as conn:
        try:
            with conn.begin():
                conn.execute(text("INSERT INTO processed_files (file_id, file_name) VALUES (:fid, :fname)"),
                             {"fid": file_id, "fname": file_name})
            return True
        except sqlalchemy.exc.IntegrityError:
            # The transaction is rolled back automatically by conn.begin()
            return False

def extract_and_ingest(file_path):
    print(f"[{os.getpid()}] Decompressing and parsing {os.path.basename(file_path)}...")
    with gzip.open(file_path, 'rt') as f:
        df = pd.read_csv(f, dtype=str, sep=',')
    print(f"[{os.getpid()}] Parsed {len(df)} rows from {os.path.basename(file_path)}.")

    print(f"[{os.getpid()}] Transforming data for {os.path.basename(file_path)}...")
    # Rename columns to match DB schema
    df = df.rename(columns={
        "device.device_uuid": "device_device_uuid",
        "device.x_device_model_name": "device_x_device_model_name",
        "device.device_status.arrival": "device_device_status_arrival",
        "device.device_status.condition": "device_device_status_condition",
        "device.device_status.strikes": "device_device_status_strikes",
        "location.location_uuid": "location_location_uuid",
        "location.location_wkt": "location_location_wkt",
        "species.species_uuid": "species_species_uuid",
        "species.x_species_name": "species_x_species_name",
        "species.species_sex": "species_species_sex",
        "species.species_age": "species_species_age",
    })

    # Convert date columns to date type
    for col in ["date_start", "date_end"]:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # Convert device_device_status_strikes to int
    df["device_device_status_strikes"] = pd.to_numeric(df["device_device_status_strikes"], errors='coerce').fillna(0).astype(int)

    # Insert JSON columns as JSON string
    df["substance"] = df["substance"].fillna("[]")
    print(f"[{os.getpid()}] Transformation complete for {os.path.basename(file_path)}.")

    print(f"[{os.getpid()}] Inserting {len(df)} rows into the database...")
    # Insert into DB
    df.to_sql("trap_data", engine, if_exists="append", index=False, method='multi')
    print(f"[{os.getpid()}] Inserted {len(df)} rows from {os.path.basename(file_path)}.")

def process_file(file_name):
    """Worker function to process a single file."""
    file_path = os.path.join(OFFLINE_DIR, file_name)

    with open(file_path, 'rb') as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()

    if mark_file_processed(file_hash, file_name):
        print(f"Claimed file {file_name} for processing.")
        extract_and_ingest(file_path)
        print(f"Finished processing {file_name}.")
    else:
        print(f"Skipping already processed file: {file_name} (hash: {file_hash})")


def main():
    print("Listing files in offline folder...")
    files = list_local_files(OFFLINE_DIR)
    print(f"Found {len(files)} .csv.gz files.")

    if not files:
        print("No new files to process.")
        return

    # Use a pool of worker processes to process files in parallel
    num_processes = multiprocessing.cpu_count()
    print(f"Using {num_processes} processes to ingest data.")
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(process_file, files)

    print("Done.")

if __name__ == "__main__":
    main()
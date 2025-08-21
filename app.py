import os
import io
import pandas as pd
from sqlalchemy import create_engine, text
import time
import psycopg2
from urllib.parse import urlparse
import tarfile
import hashlib

DATABASE_URL = "postgresql://trapuser:trappass@db:5432/trapdb"
OFFLINE_DIR = "./offline"

if isinstance(DATABASE_URL, bytes):
    DATABASE_URL = DATABASE_URL.decode('utf-8')

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
    """Lists .tar.gz files in the specified directory."""
    print(f"Scanning for files in: {directory}")
    all_files = os.listdir(directory)
    print(f"Found items: {all_files}")
    files = []
    for f in all_files:
        if f.lower().endswith(".tar.gz"):
            files.append(f)
    return files

def file_already_processed(file_id):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM processed_files WHERE file_id = :fid"), {"fid": file_id})
        return result.first() is not None

def mark_file_processed(file_id, file_name):
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO processed_files (file_id, file_name) VALUES (:fid, :fname)"),
                     {"fid": file_id, "fname": file_name})
        conn.commit()

def extract_and_ingest(file_path):
    with tarfile.open(file_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.name.lower().endswith(".csv"):
                print(f"Processing CSV: {member.name}")
                f = tar.extractfile(member)
                if f is not None:
                    # Use a tab separator for this particular CSV file
                    df = pd.read_csv(f, dtype=str, sep='\t', engine='python')

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
                        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').dt.date

                    # Convert device_device_status_strikes to int
                    df["device_device_status_strikes"] = pd.to_numeric(df["device_device_status_strikes"], errors='coerce').fillna(0).astype(int)

                    # Insert JSON columns as JSON string
                    df["substance"] = df["substance"].fillna("[]")

                    # Insert into DB
                    df.to_sql("trap_data", engine, if_exists="append", index=False, method='multi')
                    print(f"Inserted {len(df)} rows.")

def main():
    print("Listing files in offline folder...")
    files = list_local_files(OFFLINE_DIR)
    print(f"Found {len(files)} .tar.gz files.")

    for file_name in files:
        file_path = os.path.join(OFFLINE_DIR, file_name)

        with open(file_path, 'rb') as f:
            file_hash = hashlib.sha256(f.read()).hexdigest()

        if file_already_processed(file_hash):
            print(f"Skipping already processed file: {file_name} (hash: {file_hash})")
            continue

        print(f"Extracting and ingesting {file_name}...")
        extract_and_ingest(file_path)

        mark_file_processed(file_hash, file_name)
        print(f"Processed and marked {file_name} (hash: {file_hash}).")

    print("Done.")

if __name__ == "__main__":
    main()
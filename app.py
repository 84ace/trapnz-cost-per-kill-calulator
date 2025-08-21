import os
import re
import requests
import zipfile
import io
import pandas as pd
from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import time
import psycopg2
from urllib.parse import urlparse

#DATABASE_URL = "postgresql://trapuser:trappass@db:5432/trapdb"
DATABASE_URL = "postgresql://trapuser:trappass@127.0.0.1:5432/trapdb"
DRIVE_FOLDER_ID = "1o6t6tFoK8t9ihUtLonHCpzMMX38qi71x"
DATA_DIR = "./data"

if isinstance(DATABASE_URL, bytes):
    DATABASE_URL = DATABASE_URL.decode('utf-8')

if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

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

import json
import gzip

def list_drive_files(folder_id):
    """Parse embedded JSON in the folder page to get file IDs and names of .csv.gz files."""
    url = f"https://drive.google.com/drive/folders/{folder_id}"
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"Failed to access folder page: {resp.status_code}")

    # Extract the JSON data embedded in the page
    m = re.search(r'window\["_DRIVE_ivd"\] = (\[.*?\]);', resp.text)
    if not m:
        print("Could not find embedded file data in folder page.")
        return []

    data_json = m.group(1)
    try:
        data = json.loads(data_json)
    except json.JSONDecodeError:
        print("Failed to parse embedded JSON data.")
        return []

    files = []
    for item in data:
        # item format: [fileId, ..., fileName, ...]
        if len(item) > 2 and isinstance(item[0], str) and isinstance(item[2], str) and item[2].lower().endswith('.csv.gz'):
            files.append({"id": item[0], "name": item[2]})
    return files

def extract_and_ingest(gz_bytes):
    with gzip.open(io.BytesIO(gz_bytes), mode='rt') as f:
        df = pd.read_csv(f, dtype=str)

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



def file_already_processed(file_id):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1 FROM processed_files WHERE file_id = :fid"), {"fid": file_id})
        return result.first() is not None

def mark_file_processed(file_id, file_name):
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO processed_files (file_id, file_name) VALUES (:fid, :fname)"),
                     {"fid": file_id, "fname": file_name})

def download_file(file_id, file_name):
    # Google Drive direct download URL pattern for files with known ID
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    resp = requests.get(download_url)
    if resp.status_code != 200:
        raise Exception(f"Failed to download file {file_name} ({file_id})")
    return resp.content

def extract_and_ingest(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for file in z.namelist():
            if file.lower().endswith(".csv"):
                print(f"Processing CSV: {file}")
                with z.open(file) as csvfile:
                    df = pd.read_csv(csvfile, dtype=str)

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
    print("Listing files in Google Drive folder...")
    files = list_drive_files(DRIVE_FOLDER_ID)
    print(f"Found {len(files)} zip files.")

    for f in files:
        if file_already_processed(f["id"]):
            print(f"Skipping already processed file: {f['name']}")
            continue

        print(f"Downloading {f['name']}...")
        zip_content = download_file(f["id"], f["name"])

        print(f"Extracting and ingesting {f['name']}...")
        extract_and_ingest(zip_content)

        mark_file_processed(f["id"], f["name"])
        print(f"Processed and marked {f['name']}.")

    print("Done.")

if __name__ == "__main__":
    main()
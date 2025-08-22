import os
import io
import json
import time
import gzip
import hashlib
import multiprocessing
from urllib.parse import urlparse

import pandas as pd
import psycopg2
import sqlalchemy.exc
from sqlalchemy import create_engine, text, event
from sqlalchemy.dialects.postgresql import JSONB

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
DATABASE_URL = "postgresql://trapuser:trappass@127.0.0.1:5432/trapdb"
# DATABASE_URL = "postgresql://trapuser:trappass@db:5432/trapdb"
OFFLINE_DIR = "./offline"

if not os.path.exists(OFFLINE_DIR):
    os.makedirs(OFFLINE_DIR)

# -----------------------------------------------------------------------------
# DB readiness
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# SQLAlchemy engine per process
# -----------------------------------------------------------------------------
_engine = None

def _conn_pid_connect(dbapi_connection, connection_record):
    # Record which PID created this DB-API connection
    connection_record.info["pid"] = os.getpid()

def _conn_pid_checkout(dbapi_connection, connection_record, connection_proxy):
    # Ensure connections aren't reused across processes
    pid = os.getpid()
    if connection_record.info.get("pid") != pid:
        raise sqlalchemy.exc.DisconnectionError(
            f"Connection record belongs to pid {connection_record.info.get('pid')}, "
            f"attempting to check out in pid {pid}"
        )

def get_engine():
    """Create (once per-process) and return a SQLAlchemy engine."""
    global _engine
    if _engine is None:
        eng = create_engine(DATABASE_URL, pool_pre_ping=True)
        # Attach listeners to this engine instance
        event.listen(eng, "connect", _conn_pid_connect)
        event.listen(eng, "checkout", _conn_pid_checkout)
        _engine = eng
    return _engine

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def list_local_files(directory):
    """Lists .csv.gz files in the specified directory."""
    files = [f for f in os.listdir(directory) if f.lower().endswith(".csv.gz")]
    files.sort()
    return files

def sha256_file(file_path, bufsize=1024 * 1024):
    """Stream the file to compute its SHA-256 without loading it all into memory."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            b = f.read(bufsize)
            if not b:
                break
            h.update(b)
    return h.hexdigest()

def mark_file_processed(file_id, file_name):
    """Try to mark a file as processed. Returns True if we inserted the row (i.e., we own it)."""
    engine = get_engine()
    with engine.connect() as conn:
        try:
            with conn.begin():
                conn.execute(
                    text("INSERT INTO processed_files (file_id, file_name) VALUES (:fid, :fname)"),
                    {"fid": file_id, "fname": file_name}
                )
            return True
        except sqlalchemy.exc.IntegrityError:
            # Someone else already inserted it
            return False

# -----------------------------------------------------------------------------
# ETL
# -----------------------------------------------------------------------------
def extract_and_ingest(file_path):
    """Read a .csv.gz via pandas in chunks, transform, and insert into Postgres."""
    pid = os.getpid()
    base = os.path.basename(file_path)
    print(f"[{pid}] Decompressing and parsing {base}...")

    total_rows = 0
    chunk_idx = 0
    engine = get_engine()

    # Read gz directly (no generator); pandas handles gzip transparently.
    for chunk in pd.read_csv(
        file_path,
        dtype=str,
        sep=',',
        compression='gzip',
        chunksize=100_000
    ):
        chunk_idx += 1
        print(f"[{pid}] Parsed chunk {chunk_idx} (+{len(chunk)} rows) from {base}")

        # Rename to match DB schema
        chunk = chunk.rename(columns={
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

        # Convert date columns to date type (safe)
        for col in ["date_start", "date_end"]:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce').dt.date

        # Convert device_device_status_strikes to int (safe)
        if "device_device_status_strikes" in chunk.columns:
            chunk["device_device_status_strikes"] = (
                pd.to_numeric(chunk["device_device_status_strikes"], errors='coerce')
                .fillna(0)
                .astype(int)
            )

        # JSONB column: parse to Python objects (lists/dicts) so psycopg2 sends proper JSON
        if "substance" in chunk.columns:
            def _parse_json(s):
                if s is None or (isinstance(s, float) and pd.isna(s)) or s == "":
                    return []
                # Handle already-JSON-like values or strings
                try:
                    return json.loads(s)
                except Exception:
                    return []
            chunk["substance"] = chunk["substance"].map(_parse_json)

        # Insert into DB
        chunk.to_sql(
            "trap_data",
            engine,
            if_exists="append",
            index=False,
            method='multi',
            # dtype only matters on table creation; kept for future-proofing
            dtype={"substance": JSONB}
        )

        total_rows += len(chunk)

    print(f"[{pid}] Finished parsing. Total rows: {total_rows}")
    print(f"[{pid}] Inserted {total_rows} rows from {base}.")

def process_file(file_name):
    """Worker function to process a single file."""
    file_path = os.path.join(OFFLINE_DIR, file_name)
    file_hash = sha256_file(file_path)

    if mark_file_processed(file_hash, file_name):
        print(f"Claimed file {file_name} for processing.")
        try:
            extract_and_ingest(file_path)
            print(f"Finished processing {file_name}.")
        except Exception as e:
            print(f"[{os.getpid()}] ERROR processing {file_name}: {e}")
            # Optional: could delete from processed_files on failure, depending on your policy
    else:
        print(f"Skipping already processed file: {file_name} (hash: {file_hash})")

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    print("Listing files in offline folder...")
    files = list_local_files(OFFLINE_DIR)
    print(f"Found {len(files)} .csv.gz files.")
    if not files:
        print("No new files to process.")
        return

    # Wait for DB exactly once (parent process)
    wait_for_db(DATABASE_URL)

    # Create tables once (parent process)
    engine = get_engine()
    with engine.begin() as conn:
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

    # Use at most one process per file
    num_processes = min(multiprocessing.cpu_count(), len(files))
    print(f"Using {num_processes} processes to ingest data.")
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(process_file, files)

    print("Done.")

if __name__ == "__main__":
    main()

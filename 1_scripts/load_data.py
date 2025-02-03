import os
import json
import logging
import psycopg2
from minio import Minio
from dotenv import load_dotenv

# Get absolute paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOGS_DIR = os.path.join(BASE_DIR, "5_logs")

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)

# Load environment variables
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Configure logging
log_file = os.path.join(LOGS_DIR, "load_data.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MinIO credentials
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "raw-data"
FILE_NAME = "sports_data.json"

# PostgreSQL credentials
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

def download_from_minio():
    """Download JSON data from MinIO."""
    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        local_file_path = os.path.join(BASE_DIR, "2_data", FILE_NAME)
        client.fget_object(BUCKET_NAME, FILE_NAME, local_file_path)
        logging.info(f"Downloaded '{FILE_NAME}' from MinIO.")
        return local_file_path

    except Exception as e:
        logging.error(f"Failed to download file from MinIO: {str(e)}")
        return None

def load_data_to_postgres(file_path):
    """Load JSON data into PostgreSQL."""
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()

        # Load JSON data
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Insert data into table
        for league in data.get("leagues", []):
            cursor.execute(
                """
                INSERT INTO sports_data (league_id, league_name, sport)
                VALUES (%s, %s, %s)
                """,
                (league["idLeague"], league["strLeague"], league["strSport"])
            )

        # Commit and close
        conn.commit()
        cursor.close()
        conn.close()

        logging.info("Sports data successfully loaded into PostgreSQL.")

    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {str(e)}")

if __name__ == "__main__":
    file_path = download_from_minio()
    if file_path:
        load_data_to_postgres(file_path)

    print("Data load completed. Check logs for details.")
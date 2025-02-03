import os
import json
import logging
import requests
from minio import Minio
from dotenv import load_dotenv

# Get absolute paths
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "2_data")
LOGS_DIR = os.path.join(BASE_DIR, "5_logs")

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

# Load environment variables
load_dotenv(os.path.join(BASE_DIR, ".env"))

# Configure logging
log_file = os.path.join(LOGS_DIR, "fetch_data.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MinIO credentials
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "raw-data"

# API URL
SPORTS_API_URL = "https://www.thesportsdb.com/api/v1/json/3/all_leagues.php"

def fetch_sports_data():
    """Fetch sports data from The Sports DB API."""
    try:
        response = requests.get(SPORTS_API_URL)
        response.raise_for_status()
        data = response.json()

        # Save data to a local JSON file (absolute path)
        raw_data_path = os.path.join(DATA_DIR, "sports_data.json")

        with open(raw_data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        logging.info(f"Sports data fetched and saved locally: {raw_data_path}")
        return raw_data_path
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch sports data: {str(e)}")
        return None

def upload_to_minio(file_path):
    """Upload the fetched data to MinIO."""
    try:
        client = Minio(
            endpoint=MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        if not client.bucket_exists(BUCKET_NAME):
            client.make_bucket(BUCKET_NAME)
            logging.info(f"Bucket '{BUCKET_NAME}' created.")

        file_name = os.path.basename(file_path)
        client.fput_object(BUCKET_NAME, file_name, file_path)
        logging.info(f"File '{file_name}' uploaded to MinIO successfully.")

    except Exception as e:
        logging.error(f"Failed to upload file to MinIO: {str(e)}")

if __name__ == "__main__":
    data_file = fetch_sports_data()
    if data_file:
        upload_to_minio(data_file)

    print("Sports data fetch and upload completed. Check logs for details.")
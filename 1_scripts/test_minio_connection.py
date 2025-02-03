import os
import logging
from minio import Minio
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

# Ensure logs directory exists
log_dir = os.path.join(os.path.dirname(__file__), "../5_logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging
log_file = os.path.join(log_dir, "test_minio.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MinIO credentials from environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "raw-data"

try:
    # Connect to MinIO
    client = Minio(
        endpoint=MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Localhost does not use SSL
    )

    # Check if bucket exists
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        logging.info(f"Bucket '{BUCKET_NAME}' created.")
    else:
        logging.info(f"Bucket '{BUCKET_NAME}' already exists.")

    # Upload test file
    test_file = "test_connection.txt"
    with open(test_file, "w") as f:
        f.write("MinIO connection successful.")

    client.fput_object(BUCKET_NAME, test_file, test_file)
    logging.info("Test file uploaded successfully.")

    # Download and verify file
    client.fget_object(BUCKET_NAME, test_file, f"downloaded_{test_file}")
    with open(f"downloaded_{test_file}", "r") as f:
        if f.read() == "MinIO connection successful.":
            logging.info("Test file verified successfully.")
        else:
            logging.error("Test file verification failed.")

except Exception as e:
    logging.error(f"MinIO connection test failed: {str(e)}")

print("MinIO connection test completed. Check logs for details.")
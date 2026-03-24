from params.params import BASE_URL, SRC_BUCKET_NAME

from google.cloud import storage
from datetime import datetime, timedelta
import requests
import logging 
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_to_gcs(date: str, hour: int) -> None:

	pd_date = pd.Timestamp(date)

	year = pd_date.strftime("%Y")
	month = pd_date.strftime("%m")
	day = pd_date.strftime("%d")

	url = f"{BASE_URL}/{date}-{hour}.json.gz"
	blob_path = f"{year}/{month}/{day}/{hour}.json.gz"

	#storage_client = create_storage_client()
	bucket = get_bucket()

	try:
		logger.info("==================================================")
		logger.info(f"Starting Download - {date}-{hour}.json.gz")
		logger.info(f"___URL___: {url}")

		response = requests.get(url)
		response.raise_for_status()

		logger.info(f"Starting upload to {SRC_BUCKET_NAME}")

		blob = bucket.blob(blob_path)
		blob.upload_from_string(response.content)

		if not bucket.get_blob(blob_path):
			logger.error(f"___ERROR___: upload failed ---> {blob_path} does not exist.")
			raise FileNotFoundError(f"___ERROR___: {blob_path} not found in {bucket}")

	except requests.HTTPError as e:
		logger.error(f"___ERROR___: {e}")
		raise

	except Exception as e:
		logger.error(f"___ERROR___: {e}")
		raise

def create_storage_client():
	return storage.Client()


def create_gcs_bucket(client, bucket_name):
	client.create_bucket(bucket_name)

	return None


def get_bucket():
	storage_client = create_storage_client()

	if storage_client.lookup_bucket(SRC_BUCKET_NAME):
		bucket = storage_client.bucket(SRC_BUCKET_NAME)
	else:
		logger.warning("___WARNING___: Bucket does not exist.")
		logger.info(f"___CREATING BUCKET___: {SRC_BUCKET_NAME}")

		create_gcs_bucket(storage_client, SRC_BUCKET_NAME)

		logger.info(f"___BUCKET CREATION COMPLETE___: {SRC_BUCKET_NAME}")

		bucket = get_bucket()

	return bucket


if __name__ == "__main__":

	now = datetime.utcnow()
	target_time = now - timedelta(hours=1)

	date = target_time.strftime("%Y-%m-%d")
	hour = target_time.hour

	logger.info(f"Processing github archive file for {date}-{hour}")

	start_date = "2011-02-12"
	end_date = "2011-12-31"

	date_range = pd.date_range(start_date, end_date, freq="D")

	extract_to_gcs(str(date), hour)

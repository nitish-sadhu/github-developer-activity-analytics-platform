from google.cloud import storage
import requests
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_BUCKET = "raw-github-dev-activity"
BASE_URL = "https://data.gharchive.org"


def extract_to_gcs(date: str, hour: int) -> None:

	storage_client = storage.Client()
	bucket = storage_client.bucket(BASE_BUCKET)

	url = f"{BASE_URL}/{date}-{hour}.json.gz"
	blob_path = f"{date}/{date}-{hour}.json.gz"


	try:

		logger.info(f"Starting Download - {date}-{hour}.json.gz")

		response = requests.get(url)
		response.raise_for_status()

		logger.info(f"Starting upload to {BASE_BUCKET}")

		blob = bucket.blob(blob_path)
		blob.upload_from_string(response.content)

	except requests.HTTPError as e:
		logger.error(f"__ERROR__ : {e}")
		raise

	except Exception as e:
		logger.error(f"___ERROR___ : {e}")
		raise



if __name__ == "__main__":
# 2/12/2011
	extract_to_gcs("2010-01-01", "0")
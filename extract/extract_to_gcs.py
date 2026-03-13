from google.cloud import storage
from datetime import datetime, timedelta
import requests
import logging 
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_BUCKET = "raw-github-dev-activity"
BASE_URL = "https://data.gharchive.org"


def extract_to_gcs(date: str, hour: int) -> None:

	storage_client = storage.Client()
	bucket = storage_client.bucket(BASE_BUCKET)

	url = f"{BASE_URL}/{date}-{hour}.json.gz"
	blob_path = f"{hour}.json.gz"


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
	# extract_to_gcs("2010-01-01", "0")
	if True:
		now = datetime.utcnow()
		target_time = now - timedelta(hours=1)

		date = target_time.strftime("%Y-%m-%d")
		hour = target_time.hour

		logger.info(f"Processing github archive file for {date}-{hour}")

		extract_to_gcs(date, hour)

	if False:
		start_date = pd.Timestamp("2011-02-28")
		end_date = pd.Timestamp("2011-02-28")

		date_range = pd.date_range(start_date, end_date, freq="D")

		hours = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23}

		for date in date_range:
			date = date.strftime("%Y-%m-%d")
			for hour in hours:
				logger.info(f"Processing github archive file for {date}-{hour}")
				extract_to_gcs(date, hour)
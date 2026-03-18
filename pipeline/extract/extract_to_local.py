from google.cloud import storage
from datetime import datetime, timedelta
import requests
import logging 
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


BASE_URL = "https://data.gharchive.org"


def extract_to_gcs(date: str, hour: int) -> None:

	url = f"{BASE_URL}/{date}-{hour}.json.gz"

	try:
		logger.info("==================================================")
		logger.info(f"___STARTING DOWNLOAD___: {date}-{hour}.json.gz")
		logger.info(f"___URL___: {url}")

		response = requests.get(url)

		with open("/tmp_files/tmp.json.gz", "wb") as file:
			with requests.get(url) as response:
				response.raise_for_status()
				file.write(response.content)

		logger.info(f"___DOWNLOADED___: {url}")


	except requests.HTTPError as e:
		logger.error(f"___ERROR___: {e}")
		raise

	except Exception as e:
		logger.error(f"___ERROR___: {e}")
		raise


	return None


"""
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
	
"""

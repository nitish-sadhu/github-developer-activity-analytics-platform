from params.params import BASE_URL, TMP_FILES_PATH

import requests
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#BASE_URL = "https://data.gharchive.org"


def extract_to_local(date: str, hour: int) -> None:


	url = f"{BASE_URL}/{date}-{hour}.json.gz"

	try:
		logger.info("==================================================")
		logger.info(f"___STARTING DOWNLOAD___: {date}-{hour}.json.gz")
		logger.info(f"___URL___: {url}")


		with open(f"{TMP_FILES_PATH}/tmp.json.gz", "wb") as file:
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


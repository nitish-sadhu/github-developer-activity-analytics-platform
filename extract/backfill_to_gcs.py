import logging

from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from extract_to_gcs import extract_to_gcs  



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


START_DATE = "2011-02-12"
END_DATE = "2026-03-12"



def backfill(date_range:list) -> None:
	
	tasks = []


	for date in date_range:
		for hour in range(24):
			tasks.append((date, hour))

	try:

		logger.info(f"BACKFILL from {START_DATE} to {END_DATE} STARTED")

		with ThreadPoolExecutor(max_workers=10) as executor:
			executor.map(worker, tasks)

	except Exception as e:
		logger.error(f"__ERROR__: {e}")
		raise


	return None	



def worker(task:list) -> None:

	date, hour = task 
	extract_to_gcs(date, hour)

	return None



if __name__ == "__main__":

	date_range = pd.date_range(START_DATE, END_DATE, freq="D")

	backfill(date_range)



from google.cloud import storage

import json 
import requests
import pandas as pd 
from pathlib import Path 

import logging 


logger = loggin.getLogger(__name__)

BASE_BUCKET_PATH = "gcs://raw-github-dev-activity"
BASE_URL = "https://data.gharchive.org"


def download(date: str, hour: int) -> None:

	bucket_path = Path(BASE_GCS_PATH) / date / hour
	url = f"{BASE_URL}/{date}-{hour}.json.gz"

	try:
		with requests.get(url, "wb") as f:
			f.write(bucket_path)

	except Exception as e:
		logger.error(f"___ERROR___ : {e}")




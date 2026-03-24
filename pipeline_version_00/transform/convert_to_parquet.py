from params.params import TMP_FILES_PATH, TGT_BUCKET_NAME, SRC_BUCKET_NAME

from google.cloud import storage
from pyspark.sql import SparkSession
import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def create_sparksession() -> SparkSession:

	return SparkSession.builder \
			.appName("select_cols.py") \
			.master("local[*]") \
			.getOrCreate()


def create_gcs_bucket(client, bucket_name):

		client.create_bucket(bucket_name)

		return None


def create_storage_client():

	return storage.Client()


def upload_to_gcs(tgt_blob_path, tgt_bucket):

	parquet_dir = Path(TMP_FILES_PATH) / "tmp_parquet"



	for file in parquet_dir.glob("*.parquet"):
		logger.info(f"Started upload: {file}")
		tgt_blob = tgt_bucket.blob(f"{tgt_blob_path}/{file.name}")
		tgt_blob.upload_from_filename(file)
		logger.info(f"Upload complete: {file}")

	return None



def convert_to_parquet(date, hour) -> None:

	year = pd.Timestamp(date).strftime("%Y")
	month = pd.Timestamp(date).strftime("%m")
	day = pd.Timestamp(date).strftime("%d")


	src_blob_path = f"{year}/{month}/{day}/{hour}.json.gz"
	tgt_blob_path = f"{year}/{month}/{day}/{hour}"


	storage_client = create_storage_client()

	if not storage_client.lookup_bucket(TGT_BUCKET_NAME):
		create_gcs_bucket(storage_client, TGT_BUCKET_NAME)

	else:
		logger.info(f"----- {TGT_BUCKET_NAME} - already exists -----")


	src_bucket = storage_client.get_bucket(SRC_BUCKET_NAME)
	tgt_bucket = storage_client.get_bucket(TGT_BUCKET_NAME)

	src_blob = src_bucket.blob(src_blob_path)
	src_blob.download_to_filename(f"{TMP_FILES_PATH}/tmp.json.gz")


	spark = create_sparksession()

	df = spark.read.json(f"{TMP_FILES_PATH}/tmp.json.gz")
	df.write.mode("overwrite").parquet(f"{TMP_FILES_PATH}/tmp_parquet")

	logger.info(f"Converted {year}/{month}/{day}/{hour}.json.gz to parquet.")

	upload_to_gcs(tgt_blob_path, tgt_bucket)


	return None



if __name__ == "__main__":

	now = datetime.utcnow()
	tgt_time = now - timedelta(hours=1)

	hour = tgt_time.hour

	start_date = "2011-02-12"
	end_date = "2011-12-31"

	date_range = pd.date_range(start_date, end_date, freq="D")

	for date in date_range:
		for hour in range(24):
			convert_to_parquet(str(date), hour)
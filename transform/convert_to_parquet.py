from google.cloud import storage 
from pyspark.sql import SparkSession
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


TMP_FILES_PATH = "/home/krishna/github-developer-activity-project/tmp_files"
SRC_BUCKET_NAME = "raw-github-dev-activity"
TGT_BUCKET_NAME = "int-gh-dev-activity-parq"


def create_SparkSession() -> SparkSession:

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

	parquet_dir = Path(TMP_FILES_PATH) / "tmp.parquet"

	for file in parquet_dir.glob("*.parquet"):
		tgt_blob = tgt_bucket.blob(f"{tgt_blob_path}/{file.name}")
		tgt_blob.upload_from_filename(file)

	return None



def convert_to_parquet(date, hour) -> None:

	year = date.strftime("%Y")
	month = date.strftime("%m")
	day = date.strftime("%d")


	src_blob_path = f"{year}/{month}/{day}/{hour}.json.gz"
	tgt_blob_path = f"{year}/{month}/{day}/{hour}.parquet"


	storage_client = create_storage_client()

	if not storage_client.lookup_bucket(TGT_BUCKET_NAME):
		create_gcs_bucket(storage_client, TGT_BUCKET_NAME)

	else:
		logger.info(f"----- {TGT_BUCKET_NAME} - already exists -----")


	src_bucket = storage_client.get_bucket(SRC_BUCKET_NAME)
	tgt_bucket = storage_client.get_bucket(TGT_BUCKET_NAME)

	src_blob = src_bucket.blob(src_blob_path)
	src_blob.download_to_filename(f"{TMP_FILES_PATH}/tmp.json.gz")


	spark = create_SparkSession()

	df = spark.read.json(f"{TMP_FILES_PATH}/tmp.json.gz")
	df.write.parquet(f"{TMP_FILES_PATH}/tmp.parquet")


	upload_to_gcs(tgt_blob_path, tgt_bucket)


	return None



if __name__ == "__main__":

	now = datetime.utcnow()
	tgt_time = now - timedelta(hours=1)

	hour = tgt_time.hour

	convert_to_parquet(now, hour) 
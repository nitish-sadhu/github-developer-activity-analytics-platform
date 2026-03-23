from pipeline_version_01.params.params import TMP_FILES_PATH, TGT_BUCKET_NAME

from google.cloud import storage
import logging
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#TGT_BUCKET_NAME = "raw-gh-dev-activity-parq"
#TMP_FILES_PATH = "/Users/krishnasadhu/gh-dev-activity-analytics/tmp_files"


def create_gcs_bucket(client, bucket_name):
    client.create_bucket(bucket_name)
    return None


def create_storage_client():
    return storage.Client()


def get_bucket():
    storage_client = create_storage_client()

    if storage_client.lookup_bucket(TGT_BUCKET_NAME):
        bucket = storage_client.bucket(TGT_BUCKET_NAME)
    else:
        logger.warning("___WARNING___: Bucket does not exist.")
        logger.info(f"___CREATING BUCKET___: {TGT_BUCKET_NAME}")

        create_gcs_bucket(storage_client, TGT_BUCKET_NAME)

        logger.info(f"___BUCKET CREATION COMPLETE___: {TGT_BUCKET_NAME}")

        bucket = get_bucket()

    return bucket


def upload_to_gcs(date, hour):

    parquet_dir = Path(TMP_FILES_PATH) / "tmp_parquet"

    year = pd.Timestamp(date).strftime("%Y")
    month = pd.Timestamp(date).strftime("%m")
    day = pd.Timestamp(date).strftime("%d")

    tgt_blob_path = f"{year}/{month}/{day}/{hour}"

    for file in parquet_dir.glob("*.parquet"):
        logger.info(f"Started upload: {file}")

        tgt_bucket = get_bucket()
        tgt_blob = tgt_bucket.blob(f"{tgt_blob_path}/{file.name}")
        tgt_blob.upload_from_filename(file)
        logger.info(f"Upload complete: {file}")

    return None


from google.cloud import storage
import pandas as pd
import logging

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw-github-dev-activity"

def clean_gcs_bucket(date, hour) -> None:

    year = pd.Timestamp(date).strftime("%Y")
    month = pd.Timestamp(date).strftime("%m")
    day = pd.Timestamp(date).strftime("%d")

    blob_path = f"{year}/{month}/{day}/{hour}.json.gz"

    client = storage_client()

    #try:
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.get_blob(blob_path)

    blob.delete()

    if blob.exists():
        logger.info("____FAILED____: Delete failed.")
        raise

    #except Exception as e:
        logger.error(f"___ERROR___: {e}")


    return None


def storage_client():

    return storage.Client()






#!/opt/homebrew/bin/python3
import pipeline_version_01.transform.convert_to_parquet as ctp
import pipeline_version_01.extract.extract_to_local as etl
import pipeline_version_01.load.upload_to_gcs as utg

from params.params import TGT_BUCKET_NAME

import logging
import pandas as pd


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":

    start_date = "2012-02-05"
    end_date = "2012-02-29"

    date_range = pd.date_range(start_date, end_date, freq="D")

    for date in date_range:
        for hour in range(13,24):

            date = str(pd.Timestamp(date).strftime("%Y-%m-%d"))

            logger.info(f"___STARTED___: extracting to local, date: {date}, hour: {hour}")
            etl.extract_to_local(date, hour)
            logger.info(f"___COMPLETED___: extracting to local, date: {date}, hour: {hour}")

            logger.info(f"___STARTED___: Converting to parquet, date: {date}, hour: {hour}")
            ctp.convert_to_parquet(date, hour)
            logger.info(f"___COMPLETED___: Conversion complete, date: {date}, hour: {hour}")

            logger.info(f"___STARTED___: Uploading to {TGT_BUCKET_NAME}, date: {date}, hour: {hour}")
            utg.upload_to_gcs(date, hour)
            logger.info(f"___COMPLETED___: Uploading to {TGT_BUCKET_NAME}, date: {date}, hour: {hour}")






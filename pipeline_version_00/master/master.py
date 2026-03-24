import pipeline_version_00.cleanup.clean_gcs_bucket as cub
import pipeline_version_00.transform.convert_to_parquet as ctp
import logging
import pandas as pd

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    start_date = "2011-03-19"
    end_date = "2011-12-31"

    date_range = pd.date_range(start_date, end_date, freq="D")

    for date in date_range:
        for hour in range(24):
            logger.info(f"___STARTED___: Converting to parquet, date: {date}, hour: {hour}")

            ctp.convert_to_parquet(str(date), hour)

            logger.info(f"___COMPLETED___: Conversion complete, date: {date}, hour: {hour}")


            logger.info(f"___STARTED___: Deleting file. Bucket: \"raw-github-dev-activity\", date: {date}, hour: {hour}")

            cub.clean_gcs_bucket(date, hour)

            logger.info(f"___COMPLETED___: Deletion complete. bucket: \"raw-github-dev-activity\", date: {date}, hour: {hour}")



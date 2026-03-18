import pipeline.transform.convert_to_parquet as ctp
import pipeline.extract.extract_to_local as etl
import pipeline.load.upload_to_gcs as utg
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == "__main__":

    start_date = "2011-02-12"
    end_date = "2011-02-28"

    date_range = pd.date_range(start_date, end_date, freq="D")

    for date in date_range:
        for hour in range(24):

            date = str(pd.Timestamp(date).strftime("%Y-%m-%d"))

            logger.info(f"___STARTED___: extracting to local, date: {date}, hour: {hour}")
            etl.extract_to_local(date, hour)
            logger.info(f"___COMPLETED___: extracting to local, date: {date}, hour: {hour}")

            logger.info(f"___STARTED___: Converting to parquet, date: {date}, hour: {hour}")
            ctp.convert_to_parquet(date, hour)
            logger.info(f"___COMPLETED___: Conversion complete, date: {date}, hour: {hour}")

            logger.info(f"___STARTED___: Uploading to \"raw-gh-dev-activity-parq\", date: {date}, hour: {hour}")
            utg.upload_to_gcs(date, hour)
            logger.info(f"___COMPLETED___: Uploading to \"raw-gh-dev-activity-parq\", date: {date}, hour: {hour}")






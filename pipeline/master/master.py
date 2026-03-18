#import cleanup.clean_gcs_bucket as cub
import pipeline.transform.convert_to_parquet as ctp
import logging
import pandas as pd

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    start_date = "2011-07-16"
    end_date = "2021-01-31"

    date_range = pd.date_range(start_date, end_date, freq="D")

    for date in date_range:
        for hour in range(24):
            logger.info(f"___STARTED___: Converting to parquet, date: {date}, hour: {hour}")

            ctp.convert_to_parquet(str(date), hour)

            logger.info(f"___COMPLETED___: Conversion complete, date: {date}, hour: {hour}")






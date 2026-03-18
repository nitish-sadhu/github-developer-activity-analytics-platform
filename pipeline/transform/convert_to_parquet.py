from params.params import TMP_FILES_PATH

from pyspark.sql import SparkSession
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#TMP_FILES_PATH = "/Users/krishnasadhu/gh-dev-activity-analytics/tmp_files"


def create_sparksession() -> SparkSession:

	return SparkSession.builder \
			.appName("select_cols.py") \
			.master("local[*]") \
			.getOrCreate()


def convert_to_parquet(date, hour) -> None:

	year = pd.Timestamp(date).strftime("%Y")
	month = pd.Timestamp(date).strftime("%m")
	day = pd.Timestamp(date).strftime("%d")

	spark = create_sparksession()

	logger.info(f"___STARTING CONVERSION TO PARQUET___: {year}/{month}/{day}/{hour}.json.gz")

	df = spark.read.json(f"{TMP_FILES_PATH}/tmp.json.gz")
	df.write.mode("overwrite").parquet(f"{TMP_FILES_PATH}/tmp_parquet")

	logger.info(f"___CONVERTED TO PARQUET___: {year}/{month}/{day}/{hour}.json.gz")

	return None



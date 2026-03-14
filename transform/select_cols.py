import pyspark
from pyspark.sql import SparkSession

import logging


logger = logging.getLogger(__name__)


spark = SparkSession.builder \
		.appName("select_cols.py")
		.master("local[*]")
		.getOrCreate()



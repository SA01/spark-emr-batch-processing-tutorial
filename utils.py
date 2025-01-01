from logging import Logger

from pyspark import SparkConf
from pyspark.sql import SparkSession

def initialize_logger(logger_name):
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(logger_name)
    return logger


def create_spark_session(app_name: str, is_local: False, logger: Logger) -> SparkSession:
    conf = (
        SparkConf()
        .set("spark.driver.memory", "8g")
        .set("spark.sql.session.timeZone", "UTC")
    )

    if is_local:
        logger.info("creating local environment")
        spark_session = SparkSession\
            .builder\
            .master("local[*]")\
            .config(conf=conf)\
            .appName(app_name) \
            .getOrCreate()
    else:
        logger.info("running on cluster")
        spark_session = SparkSession \
            .builder \
            .config(conf=conf) \
            .appName(app_name) \
            .getOrCreate()

    return spark_session


def trim_slash(text: str):
    return text[:-1] if text.endswith("/") else text
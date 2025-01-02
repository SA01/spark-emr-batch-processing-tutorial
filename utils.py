from logging import Logger

from pyspark import SparkConf
from pyspark.sql import SparkSession


def initialize_logger(logger_name) -> Logger:
    """
    Initializes and configures a logger with the specified name.

    Args:
        logger_name (str): The name of the logger.

    Returns:
        Logger: A configured logger instance.
    """
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(logger_name)
    return logger


def create_spark_session(app_name: str, is_local: bool, logger: Logger) -> SparkSession:
    """
    Creates and configures a SparkSession based on the environment.

    Args:
        app_name (str): The name of the Spark application.
        is_local (bool): Flag indicating whether to run Spark in local mode.
        logger (Logger): The logger instance for logging information.

    Returns:
        SparkSession: An active SparkSession instance.
    """
    # Set up Spark configuration with driver memory and timezone settings
    conf = (
        SparkConf()
        .set("spark.driver.memory", "8g")  # Allocate 8 GB of memory to the Spark driver
        .set("spark.sql.session.timeZone", "UTC")  # Set the session timezone to UTC
    )

    if is_local:
        logger.info("Creating Spark session in local mode")

        # Initialize SparkSession for local development with all available cores
        spark_session = SparkSession \
            .builder \
            .master("local[*]")\
            .config(conf=conf) \
            .appName(app_name) \
            .getOrCreate()
    else:
        logger.info("Creating Spark session for cluster")

        # Initialize SparkSession for cluster execution without specifying master
        spark_session = SparkSession \
            .builder \
            .config(conf=conf) \
            .appName(app_name) \
            .getOrCreate()

    return spark_session


def trim_slash(text: str) -> str:
    """
    Removes the trailing slash from a string if it exists.

    Args:
        text (str): The input string potentially ending with a slash.

    Returns:
        str: The string without a trailing slash.
    """
    return text[:-1] if text.endswith("/") else text

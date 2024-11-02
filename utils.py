from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_spark_session(app_name: str, is_local: False) -> SparkSession:
    conf = (
        SparkConf()
        .set("spark.driver.memory", "8g")
        .set("spark.sql.session.timeZone", "UTC")
    )

    if is_local:
        spark_session = SparkSession\
            .builder\
            .master("local[*]")\
            .config(conf=conf)\
            .appName(app_name) \
            .getOrCreate()
    else:
        spark_session = SparkSession \
            .builder \
            .config(conf=conf) \
            .appName(app_name) \
            .getOrCreate()

    return spark_session
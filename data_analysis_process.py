import argparse
import os

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import create_spark_session, initialize_logger

logger = initialize_logger()

def tips_by_dropoff_zone(data: DataFrame) -> DataFrame:
    result = (
        data
        .withColumn(
            "has_tipped",
            F.when(
                (F.col("tip_amount").isNotNull()) & (F.col("tip_amount") > 0),
                F.lit(1)
            )
            .otherwise(F.lit(0))
        )
        .groupby("dropoff_service_zone", "dropoff_borough", "dropoff_zone")
        .agg(
            F.avg(F.col("tip_amount")).alias("avg_tip"),
            F.sum(F.col("has_tipped")).alias("total_tippers"),
            F.count(F.lit(1)).alias("num_rows")
        )
        .filter(F.col("num_rows") > 10)
        .withColumn("proportion_tipped", F.col("total_tippers") / F.col("num_rows"))
        .sort(F.col("proportion_tipped").desc())
    )

    return result


def average_fare_by_destination(data: DataFrame) -> DataFrame:
    result = (
        data
        .groupby("dropoff_service_zone", "dropoff_borough", "dropoff_zone")
        .agg(
            F.avg(F.col("total_amount")).alias("avg_fare"),
            F.count(F.lit(1)).alias("num_trips")
        )
        .sort(F.col("avg_fare").desc())
    )

    return result


def popular_origin_destination(data: DataFrame) -> DataFrame:
    result = (
        data
        .groupby("pickup_zone", "dropoff_zone")
        .agg(
            F.avg(F.col("total_amount")).alias("avg_fare"),
            F.avg(F.col("tip_amount")).alias("avg_tip"),
            F.avg(F.col("trip_distance")).alias("avg_distance"),
            F.count(F.lit(1)).alias("num_trips")
        )
        .sort(F.col("num_trips").desc())
    )

    return result


def parse_job_arguments() -> dict[str, str]:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--source_data_path", required=True, type=str, help="Path of source"
    )
    arg_parser.add_argument(
        "--tips_stats_path", required=True, type=str,
        help="Path to write tips statistics by destination data"
    )
    arg_parser.add_argument(
        "--avg_fare_path", required=True, type=str, help="Path to write avg fare data"
    )
    arg_parser.add_argument(
        "--popular_origin_destination_path", required=True, type=str,
        help="Path to write popular origin destination data"
    )
    job_step_args = arg_parser.parse_args()

    job_args = {
        "source_path": job_step_args.source_data_path,
        "tips_stats_path": job_step_args.tips_stats_path,
        "avg_fare_path": job_step_args.avg_fare_path,
        "popular_origin_destination_path": job_step_args.popular_origin_destination_path
    }

    return job_args

if __name__ == '__main__':
    step_args = parse_job_arguments()

    IS_LOCAL = os.getenv("LOCAL").lower() == "true"

    spark = create_spark_session(app_name="Data Aggregation", is_local=IS_LOCAL, logger=logger)

    source_data = spark.read.parquet(step_args['source_path'] + "/*.parquet")
    logger.info(f"Data count: {source_data.count()}")

    tips_stats = tips_by_dropoff_zone(data=source_data)

    logger.info(f"Writing tips stats data to {step_args['tips_stats_path']}")
    tips_stats.write.mode("overwrite").parquet(step_args['tips_stats_path'])

    avg_fare_by_destination = average_fare_by_destination(data=source_data)

    logger.info(f"Writing average fare by destination stats data to {step_args['avg_fare_path']}")
    avg_fare_by_destination.write.mode("overwrite").parquet(step_args['avg_fare_path'])

    popular_origin_destination = popular_origin_destination(data=source_data)

    logger.info(f"Writing popular origin destination data to {step_args['popular_origin_destination_path']}")
    popular_origin_destination.write.mode("overwrite").parquet(step_args['popular_origin_destination_path'])

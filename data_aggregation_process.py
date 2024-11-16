import argparse

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import create_spark_session


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
    job_step_args = arg_parser.parse_args()

    return {
        "source_path": job_step_args.source_data_path
    }


if __name__ == '__main__':
    step_args = parse_job_arguments()

    spark = create_spark_session(app_name="Data Aggregation", is_local=True)

    source_data = spark.read.parquet(step_args['source_path'] + "/*.parquet")
    print(f"Data count: {source_data.count()}")
    source_data.show(truncate=False)

    tips_stats = tips_by_dropoff_zone(data=source_data)
    tips_stats.show(truncate=False)

    avg_fare_by_destination = average_fare_by_destination(data=source_data)
    avg_fare_by_destination.show(truncate=False)

    popular_origin_destination = popular_origin_destination(data=source_data)
    popular_origin_destination.show(truncate=False)
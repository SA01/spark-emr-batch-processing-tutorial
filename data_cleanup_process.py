from typing import Callable

import argparse
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import create_spark_session, trim_slash

numeric_columns = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee"
]

def clean_numeric_fields(data: DataFrame) -> DataFrame:
    filtered_data = data
    data_columns = [str(col) for col in data.schema.names]
    for col_name in numeric_columns:
        if col_name in data_columns:
            print(f"Cleaning column {col_name}")
            filtered_data = filtered_data.filter(F.col(col_name) >= 0.0)
        else:
            print(f"Column {col_name} not found in data")

    return filtered_data


def clean_out_of_range_data(min_date_str: str, max_date_str: str) -> Callable[[DataFrame], DataFrame]:
    def clean_out_of_range_data_internal(data: DataFrame) -> DataFrame:
        print(f"Removing rows with timestamps less than {min_date_str} or greater than {max_date_str}")

        # | is "or" operation in PySpark
        out_of_range_filter = (
            (F.col("pickup_datetime") < min_date_str) |
            (F.col("pickup_datetime") > max_date_str)
        ) | (
            (F.col("dropoff_datetime") < min_date_str) |
            (F.col("dropoff_datetime") > max_date_str)
        )

        # ~ is "not" operation in PySpark
        res_data = data.filter(~out_of_range_filter)
        return res_data
    return clean_out_of_range_data_internal


def process_yellow_trips_data(data_start_time_str, data_end_time_str, data: DataFrame) -> DataFrame:
    print("Processing yellow trips data")

    res_data = (
        data
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("taxi_type", F.lit("yellow"))
        .withColumn("ehail_fee", F.lit(0.0))
        .transform(clean_numeric_fields)
        .transform(clean_out_of_range_data(data_start_time_str, data_end_time_str))
        .drop("store_and_fwd_flag", "payment_type", "RatecodeID")
    )

    return res_data

def process_green_trips_data(data_start_time_str, data_end_time_str, data: DataFrame) -> DataFrame:
    print("Processing green trips data")

    res_data = (
        data
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("taxi_type", F.lit("green"))
        .withColumn("ehail_fee", F.when(F.col("ehail_fee").isNull(), F.lit(0.0)).otherwise(F.col("ehail_fee")))
        .withColumn("airport_fee", F.lit(0.0))
        .transform(clean_numeric_fields)
        .transform(clean_out_of_range_data(data_start_time_str, data_end_time_str))
        .drop("store_and_fwd_flag", "payment_type", "RatecodeID", "trip_type")
    )

    return res_data

def attach_locations(data: DataFrame, locations_data: DataFrame) -> DataFrame:
    result_data = (
        data
            .join(locations_data, F.col("LocationID") == F.col("PULocationID"))
            .withColumnRenamed("Borough", "pickup_borough")
            .withColumnRenamed("Zone", "pickup_zone")
            .withColumnRenamed("service_zone", "pickup_service_zone")
            .drop("LocationID")
            .join(locations_data, F.col("LocationID") == F.col("DOLocationID"))
            .withColumnRenamed("Borough", "dropoff_borough")
            .withColumnRenamed("Zone", "dropoff_zone")
            .withColumnRenamed("service_zone", "dropoff_service_zone")
            .drop("LocationID")
    )

    return result_data

  
def parse_job_arguments() -> dict[str, str]:
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--yellow_trips_path", required=True, type=str, help="Path of yellow trips data"
    )
    arg_parser.add_argument(
        "--green_trips_path", required=True, type=str, help="Path of green trips data"
    )
    arg_parser.add_argument(
        "--locations_lookup_path", required=True, type=str, help="Path of locations lookup data"
    )
    arg_parser.add_argument(
        "--data_start_date", required=True, type=str, help="Start of data date (YYYY-mm-dd)"
    )
    arg_parser.add_argument(
        "--data_end_date", required=True, type=str, help="End of data date (YYYY-mm-dd)"
    )
    arg_parser.add_argument(
        "--output_path", required=True, type=str, help="Path to write output of this step"
    )

    job_step_args = arg_parser.parse_args()

    yellow_trips_path = trim_slash(job_step_args.yellow_trips_path)
    green_trips_path = trim_slash(job_step_args.green_trips_path)
    locations_lookup_path = trim_slash(job_step_args.locations_lookup_path)
    data_start_timestamp_str = f"{job_step_args.data_start_date} 00:00:00"
    data_end_timestamp_str = f"{job_step_args.data_end_date} 23:59:59"
    output_path = trim_slash(job_step_args.output_path)

    job_args = {
        "yellow_trips_path": yellow_trips_path,
        "green_trips_path": green_trips_path,
        "locations_lookup_path": locations_lookup_path,
        "data_start_timestamp_str": data_start_timestamp_str,
        "data_end_timestamp_str": data_end_timestamp_str,
        "output_path": output_path
    }

    print("Job args")
    for key, value in job_args.items():
        print(f"- {key}: {value}")

    return job_args


if __name__ == '__main__':
    step_args = parse_job_arguments()
    spark = create_spark_session(app_name="Data Cleanup", is_local=True)

    print(f"Reading Yellow trips data from {step_args['yellow_trips_path']}")
    yellow_data = (
        spark.read.parquet(step_args["yellow_trips_path"] + "/*.parquet")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    )

    print(f"Reading Green trips data from {step_args['green_trips_path']}")
    green_data = (
        spark.read.parquet(step_args["green_trips_path"] + "/*.parquet")
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    )

    print(f"Reading locations lookup data from {step_args['locations_lookup_path']}")
    locations_lookup = spark.read.option("header", "true").csv(step_args["locations_lookup_path"])

    print(f"Raw yellow data count {yellow_data.count()}")
    print(f"Raw green data count {green_data.count()}")

    yellow_data.show(truncate=False, n=10)
    green_data.show(truncate=False, n=10)

    yellow_data.summary().show(truncate=False)
    green_data.summary().show(truncate=False)

    clean_yellow_data = process_yellow_trips_data(
        data_start_time_str=step_args["data_start_timestamp_str"],
        data_end_time_str=step_args["data_end_timestamp_str"],
        data=yellow_data
    )

    clean_green_data = process_green_trips_data(
        data_start_time_str=step_args["data_start_timestamp_str"],
        data_end_time_str=step_args["data_end_timestamp_str"],
        data=green_data
    )

    clean_yellow_data.summary().show(truncate=False)
    clean_green_data.summary().show(truncate=False)

    clean_yellow_data.printSchema()
    clean_green_data.printSchema()

    combined_data = clean_yellow_data.unionByName(clean_green_data)
    data_with_locations = attach_locations(data = combined_data, locations_data=locations_lookup)

    print(f"Final data count: {data_with_locations.count()}")

    print(f"Writing prepared data to {step_args['output_path']}")
    data_with_locations.write.mode("overwrite").parquet(step_args["output_path"])

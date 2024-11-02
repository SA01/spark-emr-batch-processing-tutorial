from typing import Callable

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import create_spark_session

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


def process_yellow_trips_data(data: DataFrame) -> DataFrame:
    print("Processing yellow trips data")

    res_data = (
        data
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("ehail_fee", F.lit(0.0))
        .transform(clean_numeric_fields)
        .transform(clean_out_of_range_data("2023-01-01 00:00:00", "2023-01-31 23:59:59"))
        .drop("store_and_fwd_flag", "payment_type", "RatecodeID")
    )

    return res_data

def process_green_trips_data(data: DataFrame) -> DataFrame:
    print("Processing green trips data")

    res_data = (
        data
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("ehail_fee", F.when(F.col("ehail_fee").isNull(), F.lit(0.0)).otherwise(F.col("ehail_fee")))
        .transform(clean_numeric_fields)
        .transform(clean_out_of_range_data("2023-01-01 00:00:00", "2023-01-31 23:59:59"))
        .drop("store_and_fwd_flag", "payment_type", "RatecodeID")
    )

    return res_data


if __name__ == '__main__':
    spark = create_spark_session(app_name="Data Cleanup", is_local=True)

    yellow_data = (
        spark.read.parquet("data/yellow-taxi-trip-records/*.parquet")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    )

    green_data = (
        spark.read.parquet("data/green-taxi-trip-records/*.parquet")
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    )

    print(f"Raw yellow data count {yellow_data.count()}")
    print(f"Raw green data count {green_data.count()}")

    yellow_data.show(truncate=False, n=10)
    green_data.show(truncate=False, n=10)

    yellow_data.summary().show(truncate=False)
    green_data.summary().show(truncate=False)

    clean_yellow_data = process_yellow_trips_data(yellow_data)
    clean_green_data = process_green_trips_data(green_data)

    clean_yellow_data.summary().show(truncate=False)
    clean_green_data.summary().show(truncate=False)

    combined_data = clean_yellow_data.unionByName(clean_green_data)

    print(f"Final data count: {combined_data.count()}")


# * in green trips dataset set ehail_fee null values to 0
# * in green trips, add airport fee column, set it to 0
# * pickup relevant columns, drop store_and_fwd_flag from both sets
# * union both datasets by name
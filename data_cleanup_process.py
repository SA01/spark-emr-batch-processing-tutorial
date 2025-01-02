from typing import Callable

import os
import argparse
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import create_spark_session, trim_slash, initialize_logger

logger = initialize_logger("Data Cleanup")

numeric_columns = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee"
]


def clean_numeric_fields(data: DataFrame) -> DataFrame:
    """
    Cleans numeric fields in the DataFrame by filtering out rows where
    numeric columns have negative values.

    Parameters:
        data (DataFrame): The input PySpark DataFrame to be cleaned.

    Returns:
        DataFrame: The cleaned DataFrame with negative numeric values removed.
    """
    filtered_data = data

    # Get a list of column names present in the DataFrame as strings
    data_columns = [str(col) for col in data.schema.names]

    # Iterate over each numeric column defined earlier
    for col_name in numeric_columns:
        if col_name in data_columns:
            logger.info(f"Cleaning column {col_name}")
            filtered_data = filtered_data.filter(F.col(col_name) >= 0.0)
        else:
            logger.info(f"Column {col_name} not found in data")  # Log if column is missing

    # Return the cleaned DataFrame
    return filtered_data


def clean_out_of_range_data(min_date_str: str, max_date_str: str) -> Callable[[DataFrame], DataFrame]:
    """
    Creates a function to clean out-of-range datetime data from the DataFrame.

    Parameters:
        min_date_str (str): The minimum acceptable datetime string (inclusive).
        max_date_str (str): The maximum acceptable datetime string (inclusive).

    Returns:
        Callable[[DataFrame], DataFrame]: A function that can be used with PySpark DataFrame.transform().
    """

    def clean_out_of_range_data_internal(data: DataFrame) -> DataFrame:
        logger.info(f"Removing rows with timestamps less than {min_date_str} or greater than {max_date_str}")

        # Define a filter condition for out-of-range pickup or dropoff datetimes
        out_of_range_filter = (
                                      (F.col("pickup_datetime") < min_date_str) |
                                      (F.col("pickup_datetime") > max_date_str)
                              ) | (
                                      (F.col("dropoff_datetime") < min_date_str) |
                                      (F.col("dropoff_datetime") > max_date_str)
                              )

        # Note: NOT in PySpark is the (~) operator
        res_data = data.filter(~out_of_range_filter)
        return res_data

    # Return the internal cleaning function
    return clean_out_of_range_data_internal


def process_yellow_trips_data(data_start_time_str: str, data_end_time_str: str, data: DataFrame) -> DataFrame:
    """
    Processes yellow taxi trip data by renaming columns, adding new columns,
    cleaning numeric fields, removing out-of-range data, and dropping unnecessary columns.

    Parameters:
        data_start_time_str (str): The start datetime string for filtering data.
        data_end_time_str (str): The end datetime string for filtering data.
        data (DataFrame): The input yellow taxi trip DataFrame.

    Returns:
        DataFrame: The processed and cleaned yellow taxi trip DataFrame.
    """
    logger.info("Processing yellow trips data")

    res_data = (
        data
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")  # Rename pickup datetime column
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")  # Rename dropoff datetime column
        .withColumn("taxi_type", F.lit("yellow"))  # Add a new column indicating taxi type
        .withColumn("ehail_fee", F.lit(0.0))  # Add a new column for e-hail fee with default value 0.0
        .transform(clean_numeric_fields)  # Apply numeric field cleaning
        .transform(clean_out_of_range_data(data_start_time_str, data_end_time_str))  # Remove out-of-range data
        .drop("store_and_fwd_flag", "payment_type", "RatecodeID")  # Drop unnecessary columns
    )

    return res_data


def process_green_trips_data(data_start_time_str: str, data_end_time_str: str, data: DataFrame) -> DataFrame:
    """
    Processes green taxi trip data by renaming columns, adding or modifying columns,
    cleaning numeric fields, removing out-of-range data, and dropping unnecessary columns.

    Parameters:
        data_start_time_str (str): The start datetime string for filtering data.
        data_end_time_str (str): The end datetime string for filtering data.
        data (DataFrame): The input green taxi trip DataFrame.

    Returns:
        DataFrame: The processed and cleaned green taxi trip DataFrame.
    """
    logger.info("Processing green trips data")  # Log the start of processing

    res_data = (
        data
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")  # Rename pickup datetime column
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")  # Rename dropoff datetime column
        .withColumn("taxi_type", F.lit("green"))  # Add a new column indicating taxi type
        .withColumn(
            "ehail_fee",
            F.when(F.col("ehail_fee").isNull(), F.lit(0.0)).otherwise(F.col("ehail_fee"))
        )  # Replace null e-hail fees with 0.0
        .withColumn("airport_fee", F.lit(0.0))  # Add a new column for airport fee with default value 0.0
        .transform(clean_numeric_fields)  # Apply numeric field cleaning
        .transform(clean_out_of_range_data(data_start_time_str, data_end_time_str))  # Remove out-of-range data
        .drop("store_and_fwd_flag", "payment_type", "RatecodeID", "trip_type")  # Drop unnecessary columns
    )

    return res_data


def attach_locations(data: DataFrame, locations_data: DataFrame) -> DataFrame:
    """
    Attaches location information to the trip data by joining with the locations lookup DataFrame.

    Parameters:
        data (DataFrame): The combined yellow and green taxi trip DataFrame.
        locations_data (DataFrame): The locations lookup DataFrame containing borough and zone information.

    Returns:
        DataFrame: The trip data DataFrame enriched with pickup and dropoff location details.
    """

    result_data = (
        data
        .join(locations_data, F.col("LocationID") == F.col("PULocationID"))  # Join on pickup location ID
        .withColumnRenamed("Borough", "pickup_borough")  # Rename columns to indicate pickup location
        .withColumnRenamed("Zone", "pickup_zone")
        .withColumnRenamed("service_zone", "pickup_service_zone")
        .drop("LocationID")  # Drop redundant LocationID column
        .join(locations_data, F.col("LocationID") == F.col("DOLocationID"))  # Join on dropoff location ID
        .withColumnRenamed("Borough", "dropoff_borough")  # Rename columns to indicate dropoff location
        .withColumnRenamed("Zone", "dropoff_zone")
        .withColumnRenamed("service_zone", "dropoff_service_zone")
        .drop("LocationID")  # Drop redundant LocationID column
    )

    return result_data


def parse_job_arguments() -> dict[str, str]:
    """
    Parses command-line arguments provided to the script and prepares them for use.

    Returns:
        dict[str, str]: A dictionary containing parsed and formatted job arguments.
    """

    arg_parser = argparse.ArgumentParser(description="Data Cleanup Process Arguments")

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
    output_path = trim_slash(job_step_args.output_path)

    data_start_timestamp_str = f"{job_step_args.data_start_date} 00:00:00"
    data_end_timestamp_str = f"{job_step_args.data_end_date} 23:59:59"

    job_args = {
        "yellow_trips_path": yellow_trips_path,
        "green_trips_path": green_trips_path,
        "locations_lookup_path": locations_lookup_path,
        "data_start_timestamp_str": data_start_timestamp_str,
        "data_end_timestamp_str": data_end_timestamp_str,
        "output_path": output_path
    }

    logger.info("Job args")
    for key, value in job_args.items():
        logger.info(f"- {key}: {value}")

    return job_args


if __name__ == '__main__':
    step_args = parse_job_arguments()

    # Determine if the script is running in a local environment
    IS_LOCAL = os.getenv("LOCAL").lower() == "true"
    
    # Create a Spark session for the data analysis process
    spark = create_spark_session(app_name="Data Cleanup", is_local=IS_LOCAL)

    # Determine if the script is running in a local environment
    IS_LOCAL = os.getenv("LOCAL").lower() == "true" if os.getenv("LOCAL") else False

    # Create a Spark session
    spark = create_spark_session(app_name="Data Cleanup", is_local=IS_LOCAL, logger=logger)

    # Read the yellow trips parquet files into a DataFrame and rename datetime columns
    yellow_path = step_args["yellow_trips_path"] + "/*.parquet"
    logger.info(f"Reading Yellow trips data from {yellow_path}")
    yellow_data = (
        spark.read.parquet(yellow_path)
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    )

    # Read the green trips parquet files into a DataFrame and rename datetime columns
    green_path = step_args["green_trips_path"] + "/*.parquet"
    logger.info(f"Reading Green trips data from {green_path}")  # Log the data source path
    green_data = (
        spark.read.parquet(green_path)
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    )

    logger.info(f"Reading locations lookup data from {step_args['locations_lookup_path']}")

    # Read the locations lookup CSV file into a DataFrame with header option enabled
    locations_lookup = spark.read.option("header", "true").csv(step_args["locations_lookup_path"])

    logger.info(f"Raw yellow data count {yellow_data.count()}")
    logger.info(f"Raw green data count {green_data.count()}")

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

    # Combine the cleaned yellow and green taxi trips data into a single DataFrame and Attach location information
    combined_data = clean_yellow_data.unionByName(clean_green_data)
    data_with_locations = attach_locations(data=combined_data, locations_data=locations_lookup)

    logger.info(f"Final data count: {data_with_locations.count()}")

    # Write the final DataFrame to the specified S3 output path in Parquet format
    logger.info(f"Writing prepared data to {step_args['output_path']}")
    data_with_locations.write.mode("overwrite").parquet(step_args["output_path"])

    logger.info("Completed")
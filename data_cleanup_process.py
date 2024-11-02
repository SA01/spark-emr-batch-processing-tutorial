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



if __name__ == '__main__':
    spark = create_spark_session(app_name="Data Cleanup", is_local=True)

    yellow_data = spark.read.parquet("data/yellow-taxi-trip-records/*.parquet")
    green_data = spark.read.parquet("data/green-taxi-trip-records/*.parquet")

    print(yellow_data.count())
    print(green_data.count())

    yellow_data.show(truncate=False, n=10)
    green_data.show(truncate=False, n=10)

    yellow_data.summary().show(truncate=False)
    green_data.summary().show(truncate=False)

    clean_yellow_data = yellow_data.transform(clean_numeric_fields)
    clean_green_data = green_data.transform(clean_numeric_fields)

    clean_yellow_data.summary().show(truncate=False)
    clean_green_data.summary().show(truncate=False)


# * Clean by dates
# * in green trips dataset set ehail_fee null values to 0
# * in green trips, add airport fee column, set it to 0
# * pickup relevant columns, drop store_and_fwd_flag from both sets
# * Remove rows with negative values opf fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount,
# * congestion_surcharge, airport_fee
# * union both datasets by name
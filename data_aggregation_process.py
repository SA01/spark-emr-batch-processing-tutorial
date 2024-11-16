import argparse

from utils import create_spark_session


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

    data = spark.read.parquet(step_args['source_path'] + "/*.parquet")
    print(f"Data count: {data.count()}")
    data.show(truncate=False)

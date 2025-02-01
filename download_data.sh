#!/usr/bin/env bash

set -euo pipefail

# Usage:
#   ./download_tlc_to_s3.sh <S3_BUCKET_NAME>
#
# This script downloads yellow and green TLC data for 2022–2024
# and uploads them to the specified S3 bucket under:
#   s3://<bucket>/<taxi_type>/<year>/

if [ $# -lt 1 ]; then
  echo "Usage: $0 <S3_BUCKET_NAME>"
  exit 1
fi

S3_BUCKET="$1"

# Function to download and upload a single file
download_and_upload() {
  local year="$1"
  local month="$2"
  local taxi_type="$3"

  # Example: yellow_tripdata_2022-01.parquet
  local file_name="${taxi_type}_tripdata_${year}-${month}.parquet"
  local url="https://d37ci6vzurychx.cloudfront.net/trip-data/${file_name}"

  echo "Downloading ${url}..."
  wget -q "${url}" -O "./data/${file_name}"

  # Upload the file to S3
  # Note: This requires the AWS CLI to be installed and configured
  # with the appropriate permissions to upload to the S3 bucket.
  echo "Uploading ${file_name} to s3://${S3_BUCKET}/${taxi_type}/${year}/"
  aws s3 cp "./data/${file_name}" "s3://${S3_BUCKET}/${taxi_type}/${year}/"

  # Remove the local copy
  rm "./data/${file_name}"
}

# Loop through the months of 2022. It is possible to add multiple years here
for year in 2022; do
  if [ "$year" -eq 2024 ]; then
    # For 2024, data is available only until October (months 01–10)
    months=$(seq -w 1 10)
  else
    months=$(seq -w 1 12)
  fi

  # For each month, download both yellow and green data
  for month in $months; do
    download_and_upload "$year" "$month" "yellow"
    download_and_upload "$year" "$month" "green"
  done
done

echo "All files have been downloaded and uploaded to s3://${S3_BUCKET}."

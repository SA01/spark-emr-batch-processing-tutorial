#!/usr/bin/env bash

# Stop the script if any command fails
set -e

echo "Removing any existing package folder"
rm -rf job_package

echo "Creating a fresh package folder"
mkdir job_package

echo "Copying utils.py into job_package as common.py"
cp utils.py job_package/common.py

echo "Zipping up Python files"
zip -r -j job_zip.zip job_package/*.py

# Only run AWS upload steps if BUCKET_NAME is set
if [ -z "$BUCKET_NAME" ]; then
  echo "BUCKET_NAME is not set. Skipping S3 upload steps."
else
  echo "Uploading job_zip.zip to s3://$BUCKET_NAME/job_zip.zip..."
  aws s3 cp job_zip.zip s3://$BUCKET_NAME/job_zip.zip

  echo "Uploading data_cleanup_process.py to s3://$BUCKET_NAME/data_cleanup_process.py..."
  aws s3 cp data_cleanup_process.py s3://$BUCKET_NAME/data_cleanup_process.py

  echo "Uploading data_analysis_process.py to s3://$BUCKET_NAME/data_analysis_process.py..."
  aws s3 cp data_analysis_process.py s3://$BUCKET_NAME/data_analysis_process.py
fi

echo "Done!"

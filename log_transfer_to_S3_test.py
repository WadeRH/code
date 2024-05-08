import boto3
import json
import logging
import os
import sys

from datetime import datetime, timedelta

# def upload_files(bucket: str, path: str) -> bool:
#     """
#     Loop through file list and upload file
#     to s3 bucket
#     """
#     s3_client = boto3.client("s3")
#     tmp_files = os.listdir(tmpdir_path)

#     for file_name in tmp_files:
#         try:
#             s3_filename = path + file_name
#             s3_client.upload_file(tmpdir_path + "/" + file_name, bucket, s3_filename)
#             logger.info(f"{file_name} uploaded successfully.")
#         except Exception as e:
#            logger.exception(f"Error uploading {file_name}: {e}", exc_info=True)



# AWS credentials and region
json_file_path = r".secrets/amazon.json"

with open(json_file_path, "r") as f:
    aws_creds = json.load(f)

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
aws_region = 'us-west-2'
bucket = "livevoxcallrecordings"


log_filepath = "/home/callproc/logs/"


yesterday = datetime.now() - timedelta(1)
filedate = datetime.strftime(yesterday, "%Y%m%d")
filename = "LV_CR.log." + filedate[0:4] + "-" + filedate[4:6] + "-" + filedate[6:8]

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)


try:
    s3_filename = "logs/" + filename
    s3.upload_file(log_filepath + filename, bucket, s3_filename)
    # logger.info(f"{file_name} uploaded successfully.")
except Exception as e:
    # logger.exception(f"Error uploading {file_name}: {e}", exc_info=True)
    print("Error")
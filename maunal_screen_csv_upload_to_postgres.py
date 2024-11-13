import logging
import logging.config
import logging.handlers
import boto3
import os
import sys
import typing
import json
import psycopg2
import csv
from botocore.exceptions import ClientError
from typing import Mapping
from pathlib import Path
from os.path import join, dirname
from datetime import datetime, timedelta
from paramiko import SFTPClient, SFTPFile, Message, SFTPError, Transport
from paramiko.sftp import CMD_STATUS, CMD_READ, CMD_DATA
from zipfile import ZipFile
from logging.handlers import TimedRotatingFileHandler

# AWS credentials and region
json_file_path = r"/home/callproc/code/.secrets/amazon.json"

with open(json_file_path, "r") as f:
    aws_creds = json.load(f)

# PostgreSQL database connection details
db_host = "callrecordinginfo.cx8wkeu24exk.us-west-2.rds.amazonaws.com"
db_port = "5432"
db_name = "callrecordinginfo"
db_user = "call_recordings_user"
db_password = "ycx$qWs@nVP4T$f4"

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
aws_region = "us-west-2"

filepath = "/home/callproc/LVScreenRecordings/"
tmpdir_path = "/home/callproc/LVScreenRecordings/tmp"
log_filepath = "/home/callproc/logs/"
bucket = "livevoxscreenrecordings"

s3_client = boto3.client("s3")

# Initialize PostgreSQL connection
conn = psycopg2.connect(
    host=db_host, port=db_port, database=db_name, user=db_user, password=db_password
)
cursor = conn.cursor()

try:
    # Download CSV file from S3
    # logger.info("Getting CSV file from S3 bucket")
    s3_object = s3_client.get_object(Bucket=bucket, Key="2024/11/11/20241111_23.csv")
    csv_data = s3_object["Body"].read().decode("utf-8").splitlines()

    # Parse CSV data and insert into PostgreSQL table
    # logger.info("Reading CSV file")
    csv_reader = csv.reader(csv_data)
    next(csv_reader)  # Skip header row if present
    # logger.info("Starting data insertion to database")
    for row in csv_reader:
        # Assuming your table schema matches the CSV file columns
        cursor.execute(
            "INSERT INTO livevox_screen_metadata (recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
            ),
        )

    # Commit the transaction
    # logger.info("Commiting data to database")
    conn.commit()
    # logger.info("Data imported successfully!")

except Exception as e:
    print(f"Error: {e}")
    # send_sns_notification(
    #    error_topic_arn, "ERROR: LiveVox screen recording metadata import failed!"
    # )
    conn.rollback()

finally:
    # Close PostgreSQL connection
    cursor.close()
    conn.close()

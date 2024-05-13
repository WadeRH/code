#!/usr/bin/env python3


# import libraries

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

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
aws_region = 'us-west-2'

filepath = "/home/callproc/LVCallRecordings/"
tmpdir_path = "/home/callproc/LVCallRecordings/tmp"
log_filepath = "/home/callproc/logs/"
bucket = "livevoxcallrecordings"

error_topic_arn = 'arn:aws:sns:us-west-2:416360478487:Recording_Import_Notifications'

# PostgreSQL database connection details
db_host = 'callrecordinginfo.cx8wkeu24exk.us-west-2.rds.amazonaws.com'
db_port = '5432'
db_name = 'callrecordinginfo'
db_user = 'call_recordings_user'
db_password = 'ycx$qWs@nVP4T$f4'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.handlers.TimedRotatingFileHandler(
    "/home/callproc/logs/LV_CR.log", when="midnight", backupCount=14
)
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
# create formatter and add it to the handlers
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)


def setup_logging(
    default_path="logging.json", default_level=logging.INFO, env_key="LOG_CFG"
):
    """Setup logging configuration"""
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def get_call_recordings():
    """
    The script contains example of the paramiko usage for large file downloading.
    It implements :func:`download` with limited number of concurrent requests to server, whereas
    paramiko implementation of the :meth:`paramiko.SFTPClient.getfo` send read requests without
    limitations, that can cause problems if large file is being downloaded.
    """

    # Create filename to download based on yesterday's date
    yesterday = datetime.now() - timedelta(1)
    filename = datetime.strftime(yesterday, "%Y%m%d") + "_RECORDINGS.zip"
    file_path = "/home/callproc/LVCallRecordings/"
    host = "bob.na6.livevox.com"
    port = 22
    username = "LV_NA6_0043"
    password = "Lv_NA^_0043"
    remote_file_path = filename

    logger.info("Starting file download")

    transport = Transport((host, port))
    transport.set_keepalive(30)
    transport.connect(
        username=username,
        password=password,
    )
    with SFTPClient.from_transport(transport) as sftp_client:
        progress_size = 0
        total_size = 0
        step_size = 4 * 1024 * 1024

        def progress_callback(data):
            nonlocal progress_size, total_size
            progress_size += len(data)
            total_size += len(data)
            while progress_size >= step_size:
                logger.info(f"{total_size // (1024 ** 2)} MB has been downloaded")
                progress_size -= step_size

        download_file(
            sftp_client=sftp_client,
            remote_path=remote_file_path,
            local_path=file_path + filename,
            callback=progress_callback,
        )


class _SFTPFileDownloader:
    """
    Helper class to download large file with paramiko sftp client with limited number of concurrent requests.
    """

    _DOWNLOAD_MAX_REQUESTS = 48
    _DOWNLOAD_MAX_CHUNK_SIZE = 0x8000

    def __init__(self, f_in: SFTPFile, f_out: typing.BinaryIO, callback=None):
        self.f_in = f_in
        self.f_out = f_out
        self.callback = callback

        self.requested_chunks = {}
        self.received_chunks = {}
        self.saved_exception = None

    def download(self):
        file_size = self.f_in.stat().st_size
        requested_size = 0
        received_size = 0

        while True:
            # send read requests
            while (
                len(self.requested_chunks) + len(self.received_chunks)
                < self._DOWNLOAD_MAX_REQUESTS
                and requested_size < file_size
            ):
                chunk_size = min(
                    self._DOWNLOAD_MAX_CHUNK_SIZE, file_size - requested_size
                )
                request_id = self._sftp_async_read_request(
                    fileobj=self,
                    file_handle=self.f_in.handle,
                    offset=requested_size,
                    size=chunk_size,
                )
                self.requested_chunks[request_id] = (requested_size, chunk_size)
                requested_size += chunk_size

            # receive blocks if they are available
            # note: the _async_response is invoked
            self.f_in.sftp._read_response()
            self._check_exception()

            # write received data to output stream
            while True:
                chunk = self.received_chunks.pop(received_size, None)
                if chunk is None:
                    break
                _, chunk_size, chunk_data = chunk
                self.f_out.write(chunk_data)
                if self.callback is not None:
                    self.callback(chunk_data)

                received_size += chunk_size

            # check transfer status
            if received_size >= file_size:
                break

            # check chunks queues
            if (
                not self.requested_chunks
                and len(self.received_chunks) >= self._DOWNLOAD_MAX_REQUESTS
            ):
                raise ValueError(
                    "SFTP communication error. The queue with requested file chunks is empty and"
                    "the received chunks queue is full and cannot be consumed."
                )

        return received_size

    def _sftp_async_read_request(self, fileobj, file_handle, offset, size):
        sftp_client = self.f_in.sftp

        with sftp_client._lock:
            num = sftp_client.request_number

            msg = Message()
            msg.add_int(num)
            msg.add_string(file_handle)
            msg.add_int64(offset)
            msg.add_int(size)

            sftp_client._expecting[num] = fileobj
            sftp_client.request_number += 1

        sftp_client._send_packet(CMD_READ, msg)
        return num

    def _async_response(self, t, msg, num):
        if t == CMD_STATUS:
            # save exception and re-raise it on next file operation
            try:
                self.f_in.sftp._convert_status(msg)
            except Exception as e:
                self.saved_exception = e
            return
        if t != CMD_DATA:
            raise SFTPError("Expected data")
        data = msg.get_string()

        chunk_data = self.requested_chunks.pop(num, None)
        if chunk_data is None:
            return

        # save chunk
        offset, size = chunk_data

        if size != len(data):
            raise SFTPError(
                f"Invalid data block size. Expected {size} bytes, but it has {len(data)} size"
            )
        self.received_chunks[offset] = (offset, size, data)

    def _check_exception(self):
        """if there's a saved exception, raise & clear it"""
        if self.saved_exception is not None:
            x = self.saved_exception
            self.saved_exception = None
            raise x


def download_file(
    sftp_client: SFTPClient, remote_path: str, local_path: str, callback=None
):
    """
    Helper function to download remote file via sftp.
    It contains a fix for a bug that prevents a large file downloading with :meth:`paramiko.SFTPClient.get`
    Note: this function relies on some private paramiko API and has been tested with paramiko 2.7.1.
          So it may not work with other paramiko versions.
    :param sftp_client: paramiko sftp client
    :param remote_path: remote file path
    :param local_path: local file path
    :param callback: optional data callback
    """
    remote_file_size = sftp_client.stat(remote_path).st_size

    logger.info("Starting file download")

    with sftp_client.open(remote_path, "rb") as f_in, open(local_path, "wb") as f_out:
        _SFTPFileDownloader(f_in=f_in, f_out=f_out, callback=callback).download()

    local_file_size = os.path.getsize(local_path)
    if remote_file_size != local_file_size:
        raise IOError(f"file size mismatch: {remote_file_size} != {local_file_size}")

    logger.info("File downloaded completed")


def generate_zip_file_name():
    # Generate file name to unzip based on yesterdays date
    yesterday = datetime.now() - timedelta(1)
    filename = datetime.strftime(yesterday, "%Y%m%d") + "_RECORDINGS.zip"
    return filename


def unzip_call_recordings():
    """\
    This script unzips the call recording ZIP file
    into a tmp directory
    """

    # Generate file name to unzip based on yesterdays date
    filename = generate_zip_file_name()

    tempdir = "tmp/"
    localpath = filepath + filename
    zippass = "livevox"

    # Load the zip file and create a zip object

    logger.info("Starting file extraction")

    with ZipFile(localpath, "r") as zObject:
        # Extract all files to temp directory
        zObject.extractall(path=tmpdir_path, pwd=bytes(zippass, "utf-8"))

    logger.info("Files unzipped.")


def upload_call_recordings():
    """\
        This script iterates through the tmp directory
        copying all files to the S3 bucket in AWS
    """
    tmp_files = os.listdir(tmpdir_path)
    year = tmp_files[0][:4]
    month = tmp_files[0][4:6]
    day = tmp_files[0][6:8]
    search_limit = tmp_files[0][0:8]
    s3path = year + "/" + month + "/" + day + "/"
    global csv_key 
    csv_key = s3path + year + month + day + ".csv"
    

    # Check if files exist in tempdir
    is_empty = not any(Path(tmpdir_path).iterdir())

    if is_empty:
        print("No files found to process.")
        logger.info("No files found to process.")
        print("Ending program")
        logger.info("Ending Program.")
        raise SystemExit()
    else:
        print("Files found to process")
        logger.info("Files found to process")

    # Connect to AWS and create an S3 resource
    s3 = boto3.resource(
        service_name="s3",
        region_name="us-west-2"
    )

    if folder_exists(bucket, s3path):
        print("Folder exists")
        logger.info("Folder exists")
    else:
        # Make appropriate path
        try:
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=bucket, Key=(s3path)
            )  # Note: No content is uploaded, this just creates the "folder" object
            print(f"Folder '{s3path}' created successfully in bucket '{bucket}'.")
            logger.info(f"Folder '{s3path}' created successfully in bucket '{bucket}'.")
        except Exception as e:
            print(f"Error creating folder '{s3path}' in bucket '{bucket}': {e}")
            logger.exception(
                f"Error creating folder '{s3path}' in bucket '{bucket}': {e}",
                exc_info=True,
            )

    # Upload files
    upload_files(bucket, s3path)

    # Get list of files from S3 bucket with first 6 that match files in tempdir
    # and trim the 8 leftmost characters (which represent the path prefix in AWS)
    s3_files = list_files_in_bucket(bucket, search_limit, prefix=s3path)

    logger.info("Files in S3:" + str(len(s3_files)))

    # Get list of files in tempdir
    local_files = read_files_in_local_folder(tmpdir_path)

    logger.info("Files in temp directory:" + str(len(local_files)))

    # Compare tempdir and bucket file lists
    differences_in_s3, differences_in_local = find_differences(s3_files, local_files)

    print("Differences in s3 files compared to local: ")
    logger.info("Differences in s3 files compared to local:" + str(len(differences_in_s3)))
    print(len(differences_in_s3))
    logger.info(len(differences_in_s3))

    print("Differences in local files compared to s3: " + str(len(differences_in_local)))
    print(len(differences_in_local))
    logger.info("Differences in local files compared to s3: ")
    logger.info(len(differences_in_local))

    # If same move on

    if len(differences_in_local) & len(differences_in_s3) == 0:
        print("No differences between files")
        print(
            "Call recordings for "
            + search_limit
            + " extracted and uploaded successfully"
        )
        print("Initiating file cleanup.")

        logger.info("No differences between files")
        logger.info(
            "Call recordings for "
            + search_limit
            + " extracted and uploaded successfully"
        )
        logger.info("Initiating file cleanup.")

        # call cleanup functions
        # Deleting files in tempdir
        # Deleting source zip file

        try:
            cleanup(filepath, tmpdir_path)
            print("File cleanup completed.")
            logger.info("File cleanup completed.")
        except Exception as e:
            print(f"An error occurred: {e}")
            logger.exception(f"An error occurred: {e}", exc_info=True)
    else:
        # Build functions to identify missing files in S3 and reupload
        send_sns_notification(error_topic_arn, "ERROR: Mismatch between files uploaded to S3 and in local drive!")
        
        logger.info("Differences in S3 compared to local: ")
        for item in differences_in_s3:
            logger.info(item)
            
        logger.info("Differences in local compared to S3 : ")
        for item in differences_in_local:
            logger.info(item)

def folder_exists(bucket: str, path: str) -> bool:
    """
    Check to see if YYYY/MM/DD diretory exists within S3 bucket
    based on first 6 characters of filename
    """
    s3 = boto3.resource(
        service_name="s3",
        region_name="us-west-2"
    )
    
    s3 = boto3.client("s3")
    path = path.rstrip("/")
    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter="/", MaxKeys=1)
    return "CommonPrefixes" in resp


def upload_files(bucket: str, path: str) -> bool:
    """
    Loop through file list and upload file
    to s3 bucket
    """
    s3_client = boto3.client("s3")
    tmp_files = os.listdir(tmpdir_path)

    for file_name in tmp_files:
        try:
            s3_filename = path + file_name
            s3_client.upload_file(tmpdir_path + "/" + file_name, bucket, s3_filename)
            logger.info(f"{file_name} uploaded successfully.")
        except Exception as e:
            logger.exception(f"Error uploading {file_name}: {e}", exc_info=True)


def list_files_in_bucket(bucket_name, filter, prefix=""):
    """
    List files in an S3 bucket with the given prefix.
    """

    files = []
    file_count = 0

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")

    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                if obj in page["Contents"]:
                    files.append(obj["Key"])
                    file_count += 1

    trimmed_files = [item[11:] for item in files]

    s3_files = [item for item in trimmed_files if item]

    return s3_files


def read_files_in_local_folder(folder_path):
    """
    Create a list of all extraced files in the tempdir.
    """
    local_files = []

    # Check if the folder exists
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        # Iterate over all files in the folder
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            # Check if it's a file (not a directory)
            if os.path.isfile(file_path):
                local_files.append((file_name))
    else:
        print(f"The folder '{folder_path}' does not exist.")
        logger.info(f"The folder '{folder_path}' does not exist.")

    return local_files


def find_differences(list1, list2):
    """
    Find differences between two lists.
    """
    set1 = set(list1)
    set2 = set(list2)

    in_s3_not_in_local = set1 - set2
    in_local_not_in_s3 = set2 - set1

    return in_s3_not_in_local, in_local_not_in_s3


def cleanup(directory, tmpdirectory):
    """
    Delete all files in a directory and the zip file
    previously downloaded.
    """
    try:
        # List all files in the directory
        files = os.listdir(tmpdirectory)

        # Iterate over each file and delete it
        for file in files:
            file_path = os.path.join(tmpdirectory, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")

        print("All extracted files deleted successfully.")
        logger.info("All extracted files deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception(f"An error occurred: {e}", exc_info=True)

    zipfile = generate_zip_file_name()

    try:
        # Delete the zip file
        file_path = os.path.join(directory, zipfile)
        if os.path.isfile(file_path):
            os.remove(file_path)
            logger.info(f"Deleted file: {file_path}")

        logger.info(zipfile + " deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception(f"An error occurred: {e}", exc_info=True)

def csv_to_postgres():
    s3_client = boto3.client("s3")
    
    # Initialize PostgreSQL connection
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    cursor = conn.cursor()

    try:
        # Download CSV file from S3
        logger.info("Getting CSV file from S3 bucket")
        s3_object = s3_client.get_object(Bucket=bucket, Key=csv_key)
        csv_data = s3_object['Body'].read().decode('utf-8').splitlines()

        # Parse CSV data and insert into PostgreSQL table
        logger.info("Reading CSV file")
        csv_reader = csv.reader(csv_data)
        next(csv_reader)  # Skip header row if present
        logger.info("Starting data insertion to database")
        for row in csv_reader:
            # Assuming your table schema matches the CSV file columns
            cursor.execute("INSERT INTO livevox_metadata (recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
        
        # Commit the transaction
        logger.info("Commiting data to database")
        conn.commit()
        logger.info("Data imported successfully!")

    except Exception as e:
        print(f"Error: {e}")
        send_sns_notification(error_topic_arn, "ERROR: LiveVox call recording metadata import failed!")
        conn.rollback()

    finally:
        # Close PostgreSQL connection
        cursor.close()
        conn.close()

def send_sns_notification(topic_arn, message):
    json_file_path = r".secrets/amazon.json"

    with open(json_file_path, "r") as f:
        aws_creds = json.load(f)

    aws_access_key_id = aws_creds["aws_access_key_id"]
    aws_secret_access_key = aws_creds["aws_secret_access_key"]
    aws_region = 'us-west-2'
    
    sns_client = boto3.client('sns', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message
    )
    
    print("MessageId of the published message:", response['MessageId'])
    logger.info("MessageId of the published message:", response['MessageId'])
    
def yesterdays_log_to_S3():
    yesterday = datetime.now() - timedelta(1)
    filedate = datetime.strftime(yesterday, "%Y%m%d")
    filename = "LV_CR.log." + filedate[0:4] + "-" + filedate[4:6] + "-" + filedate[6:8]
    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

    try:
        s3_filename = "logs/" + filename
        s3.upload_file('/home/callproc/logs/' + filename, bucket, s3_filename)
        logger.info(f"{filename} log file uploaded successfully.")
    except Exception as e:
        logger.exception(f"Error uploading {filename}: {e}", exc_info=True)

def main():

    setup_logging()

    # DOWNLOAD CALL RECORDINGS
    get_call_recordings()

    # UNZIP CALL RECORDINGS
    unzip_call_recordings()

    # UPLOAD CALL RECORDINGS
    upload_call_recordings()
    
    # IMPORT CSV TO POSTGRES
    csv_to_postgres()

    # UPLOAD YESTERDAYS LOG FILE
    yesterdays_log_to_S3()

    sys.exit(0)


if __name__ == "__main__":
    main()

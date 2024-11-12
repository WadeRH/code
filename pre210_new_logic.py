import requests
import datetime
from pathlib import Path
import psycopg2
import boto3
import logging
import logging.config
import logging.handlers
import os
import sys
from botocore.exceptions import ClientError
from logging.handlers import RotatingFileHandler
import json


# PostgreSQL database connection details
db_host = "callrecordinginfo.cx8wkeu24exk.us-west-2.rds.amazonaws.com"
db_port = "5432"
db_name = "callrecordinginfo"
db_user = "call_recordings_user"
db_password = "ycx$qWs@nVP4T$f4"

# AWS connection details
json_file_path = r".secrets/amazon.json"

with open(json_file_path, "r") as f:
    aws_creds = json.load(f)

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
region_name = "us-west-2"

tmpdir_path = "/home/callproc/pre210_callrecs"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.handlers.RotatingFileHandler(
    "/home/callproc/logs/LV_pre210.log", backupCount=14
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


def get_call_recordings(start, end):
    ### Get session token for future API calls

    session_token_endpoint = "https://api.na6.livevox.com/session/login"
    session_token_headers = {
        "LV-Access": "eefe858c-430c-4788-b5a2-7d33bb0ec611",
        "Content-Type": "application/json",
    }
    session_token_data = {
        "clientName": "LV_NA6_0043",
        "userName": "APIUSER",
        "password": "iLending092023!",
        "agent": "false",
    }

    response = requests.post(
        session_token_endpoint, headers=session_token_headers, json=session_token_data
    ).json()

    session_token = response.get("sessionId")

    ### Get call recording list from API
    ### PASS #1

    call_recording_endpoint = (
        "https://api.na6.livevox.com/reporting/standard/callRecording"
    )
    call_recording_headers = {"LV-Session": session_token}
    call_recording_data = {
        "startDate": "2023-12-23T00:00:00-05:00",
        # "startDate": start,  # 1712448000000
        "endDate": "2023-12-23T23:59:59-05:00",
        # "endDate": end,  # 1712534400000
        "sortBy": "CALL_START_TIME",
        # "filter": {"callCenter": [{"id": 3063064}]},
        "filter": {
            "service": [
                {"id": "3181203"},
                {"id": "3183318"},
                {"id": "3183512"},
                {"id": "3183866"},
                {"id": "3183867"},
                {"id": "3183682"},
                {"id": "3183683"},
                {"id": "3181622"},
                {"id": "3179521"},
                {"id": "3179531"},
                {"id": "3179529"},
                {"id": "3179528"},
                {"id": "3179526"},
                {"id": "3188568"},
                {"id": "3188569"},
                {"id": "3186816"},
                {"id": "3184265"},
                {"id": "3183868"},
                {"id": "3183869"},
                {"id": "3179522"},
                {"id": "3183863"},
                {"id": "3183862"},
                {"id": "3181231"},
                {"id": "3185044"},
                {"id": "3179527"},
                {"id": "3188521"},
                {"id": "3179532"},
                {"id": "3186521"},
                {"id": "3176372"},
                {"id": "3190211"},
                {"id": "3183864"},
                {"id": "3183865"},
                {"id": "3183860"},
                {"id": "3183861"},
                {"id": "3185132"},
                {"id": "3185152"},
                {"id": "3189863"},
                {"id": "3190200"},
                {"id": "3184376"},
                {"id": "3189321"},
                {"id": "3184342"},
                {"id": "3186013"},
                {"id": "3186525"},
                {"id": "3184399"},
                {"id": "3184923"},
                {"id": "3184405"},
                {"id": "3184406"},
                {"id": "3188731"},
                {"id": "3190004"},
                {"id": "3186526"},
                {"id": "3184407"},
                {"id": "3184387"},
                {"id": "3187313"},
                {"id": "3184408"},
                {"id": "3184394"},
                {"id": "3190997"},
                {"id": "3184388"},
                {"id": "3184384"},
                {"id": "3189888"},
                {"id": "3184366"},
                {"id": "3184364"},
                {"id": "3189890"},
                {"id": "3190994"},
                {"id": "3184392"},
                {"id": "3188629"},
                {"id": "3189880"},
                {"id": "3184911"},
                {"id": "3184435"},
                {"id": "3186002"},
                {"id": "3184369"},
                {"id": "3186140"},
                {"id": "3186001"},
                {"id": "3188727"},
                {"id": "3188628"},
                {"id": "3186012"},
                {"id": "3184422"},
                {"id": "3186011"},
                {"id": "3186527"},
                {"id": "3184433"},
                {"id": "3187381"},
                {"id": "3184389"},
                {"id": "3186015"},
                {"id": "3186005"},
                {"id": "3184365"},
                {"id": "3184393"},
                {"id": "3186524"},
                {"id": "3184373"},
                {"id": "3191000"},
                {"id": "3184651"},
                {"id": "3190995"},
                {"id": "3188712"},
                {"id": "3184355"},
                {"id": "3190998"},
                {"id": "3184925"},
                {"id": "3184439"},
                {"id": "3184409"},
                {"id": "3189854"},
                {"id": "3184379"},
                {"id": "3184391"},
                {"id": "3184915"},
                {"id": "3184378"},
                {"id": "3186008"},
                {"id": "3184910"},
                {"id": "3184441"},
                {"id": "3184429"},
                {"id": "3184398"},
                {"id": "3186007"},
                {"id": "3191001"},
                {"id": "3184385"},
                {"id": "3189886"},
                {"id": "3184926"},
                {"id": "3184432"},
                {"id": "3184410"},
                {"id": "3190070"},
                {"id": "3189881"},
            ]
        },
    }

    call_recording_report_response = requests.post(
        call_recording_endpoint,
        headers=call_recording_headers,
        json=call_recording_data,
    ).json()

    call_recording_report = call_recording_report_response.get("callRecording")

    # fieldnames = ['account', 'transferConnect', 'phone', 'session', call_result, 'termCode', 'campaign', 'serviceId', 'agent', 'transferDuration']

    # NOTE: Postgres columns = recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs
    # Mapping to sublist             13                  00              06          03          05             NULL       10                09              11        04          08

    value_list_from_call_recording_report = []

    list_for_postgres = []

    holding_list = []

    for dictionary in call_recording_report:
        # values = list(dictionary.values())
        # value_list_from_call_recording_report =

        holding_list = [
            dictionary.get("account", "0"),
            dictionary.get("service", "N/A"),
            dictionary.get("name", "N/A"),
            dictionary.get("phone", "00000000000"),
            dictionary.get("agent", "N/A"),
            dictionary.get("session", "N/A"),
            dictionary.get("transferConnect", "00000000"),
            dictionary.get("transferEnd", "00000000"),
            dictionary.get("transferDuration", 000),
            dictionary.get("campaign", "N/A"),
            dictionary.get("termCode", "N/A"),
            dictionary.get("serviceId", "000000"),
            dictionary.get("callCenterId", "000000"),
            dictionary.get("recordingId", "N/A"),
        ]

        value_list_from_call_recording_report.append(holding_list)

    for sublist in value_list_from_call_recording_report:

        # SUBLIST ORDER
        # 0 - Account
        # 1 - Service Name
        # 2 - Customer Name
        # 3 - Phone Dialed
        # 4 - Agent
        # 5 - Session ID
        # 6 - TransferConnect - Start time
        # 7 - TransferEnd - End time
        # 8 - Duration(seconds)
        # 9 - Campaign
        # 10 - TermCode
        # 11 - ServiceID
        # 12 - CallCenter ID
        # 13 - recordingID

        recordingID = sublist[13]

        calldate = datetime.datetime.fromtimestamp(int(sublist[6][:10]) - 18000)
        calldate_str = calldate.strftime("%Y%m%d%H%M%S")

        recording_filename = calldate_str + "_" + sublist[0] + "_" + sublist[3] + ".mp3"

        call_recording_dl_endpoint = (
            "https://api.na6.livevox.com/compliance/recording/" + recordingID + "?="
        )

        call_recording_binary = requests.get(
            call_recording_dl_endpoint, headers=call_recording_headers
        )

        call_recording_filename = Path(
            "/home/callproc/pre210_callrecs/" + recording_filename
        )

        call_recording_filename.write_bytes(call_recording_binary.content)

        sublist[13] = recording_filename

        ## Write fields to new list of lists for import into Postgres

        temp_list = [
            sublist[13],
            sublist[0],
            calldate_str,
            sublist[3],
            sublist[5].replace("@", "_"),
            "NA",
            sublist[10],
            sublist[9],
            sublist[11],
            sublist[4],
            sublist[8],
        ]

        list_for_postgres.append(temp_list)

    len_postgres = len(list_for_postgres)
    logger.info(f"Length of list for postgres - {len_postgres}")

    ### Get call recording list from API
    ### PASS #2

    call_recording_endpoint = (
        "https://api.na6.livevox.com/reporting/standard/callRecording"
    )
    call_recording_headers = {"LV-Session": session_token}
    call_recording_data = {
        "startDate": start,  # 1712448000000
        "endDate": end,  # 1712534400000
        "sortBy": "CALL_START_TIME",
        # "filter": {"callCenter": [{"id": 3063064}]},
        "filter": {
            "service": [
                {"id": "3189879"},
                {"id": "3184367"},
                {"id": "3184411"},
                {"id": "3188716"},
                {"id": "3186559"},
                {"id": "3184412"},
                {"id": "3185998"},
                {"id": "3184383"},
                {"id": "3184431"},
                {"id": "3188729"},
                {"id": "3189877"},
                {"id": "3184440"},
                {"id": "3190759"},
                {"id": "3184425"},
                {"id": "3184427"},
                {"id": "3189876"},
                {"id": "3189887"},
                {"id": "3184434"},
                {"id": "3184424"},
                {"id": "3184341"},
                {"id": "3184421"},
                {"id": "3188730"},
                {"id": "3184345"},
                {"id": "3184377"},
                {"id": "3184918"},
                {"id": "3184386"},
                {"id": "3190999"},
                {"id": "3186000"},
                {"id": "3186006"},
                {"id": "3184413"},
                {"id": "3186010"},
                {"id": "3189875"},
                {"id": "3189857"},
                {"id": "3189859"},
                {"id": "3189873"},
                {"id": "3184414"},
                {"id": "3189889"},
                {"id": "3190993"},
                {"id": "3191002"},
                {"id": "3190996"},
                {"id": "3184381"},
                {"id": "3184913"},
                {"id": "3184416"},
                {"id": "3184347"},
                {"id": "3188627"},
                {"id": "3184360"},
                {"id": "3184423"},
                {"id": "3188779"},
                {"id": "3190125"},
                {"id": "3184400"},
                {"id": "3184920"},
                {"id": "3190071"},
                {"id": "3184401"},
                {"id": "3189872"},
                {"id": "3184438"},
                {"id": "3184361"},
                {"id": "3189874"},
                {"id": "3189884"},
                {"id": "3184437"},
                {"id": "3190006"},
                {"id": "3184402"},
                {"id": "3184403"},
                {"id": "3184428"},
                {"id": "3187382"},
                {"id": "3189878"},
                {"id": "3187885"},
                {"id": "3190005"},
                {"id": "3189882"},
                {"id": "3191003"},
                {"id": "3184922"},
                {"id": "3184420"},
                {"id": "3184368"},
                {"id": "3184380"},
                {"id": "3184370"},
                {"id": "3184371"},
                {"id": "3187316"},
                {"id": "3185999"},
                {"id": "3184363"},
                {"id": "3184914"},
                {"id": "3184372"},
                {"id": "3189883"},
            ]
        },
    }

    call_recording_report_response = requests.post(
        call_recording_endpoint,
        headers=call_recording_headers,
        json=call_recording_data,
    ).json()

    call_recording_report = call_recording_report_response.get("callRecording")

    # fieldnames = ['account', 'transferConnect', 'phone', 'session', call_result, 'termCode', 'campaign', 'serviceId', 'agent', 'transferDuration']

    # NOTE: Postgres columns = recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs
    # Mapping to sublist             13                  00              06          03          05             NULL       10                09              11        04          08

    value_list_from_call_recording_report = []

    holding_list = []

    for dictionary in call_recording_report:
        # values = list(dictionary.values())
        # value_list_from_call_recording_report =

        holding_list = [
            dictionary.get("account", "0"),
            dictionary.get("service", "N/A"),
            dictionary.get("name", "N/A"),
            dictionary.get("phone", "00000000000"),
            dictionary.get("agent", "N/A"),
            dictionary.get("session", "N/A"),
            dictionary.get("transferConnect", "00000000"),
            dictionary.get("transferEnd", "00000000"),
            dictionary.get("transferDuration", 000),
            dictionary.get("campaign", "N/A"),
            dictionary.get("termCode", "N/A"),
            dictionary.get("serviceId", "000000"),
            dictionary.get("callCenterId", "000000"),
            dictionary.get("recordingId", "N/A"),
        ]

        value_list_from_call_recording_report.append(holding_list)

    for sublist in value_list_from_call_recording_report:

        # SUBLIST ORDER
        # 0 - Account
        # 1 - Service Name
        # 2 - Customer Name
        # 3 - Phone Dialed
        # 4 - Agent
        # 5 - Session ID
        # 6 - TransferConnect - Start time
        # 7 - TransferEnd - End time
        # 8 - Duration(seconds)
        # 9 - Campaign
        # 10 - TermCode
        # 11 - ServiceID
        # 12 - CallCenter ID
        # 13 - recordingID

        recordingID = sublist[13]

        calldate = datetime.datetime.fromtimestamp(int(sublist[6][:10]) - 18000)
        calldate_str = calldate.strftime("%Y%m%d%H%M%S")

        recording_filename = calldate_str + "_" + sublist[0] + "_" + sublist[3] + ".mp3"

        call_recording_dl_endpoint = (
            "https://api.na6.livevox.com/compliance/recording/" + recordingID + "?="
        )

        call_recording_binary = requests.get(
            call_recording_dl_endpoint, headers=call_recording_headers
        )

        call_recording_filename = Path(
            "/home/callproc/pre210_callrecs/" + recording_filename
        )

        call_recording_filename.write_bytes(call_recording_binary.content)

        sublist[13] = recording_filename

        ## Write fields to new list of lists for import into Postgres

        temp_list = [
            sublist[13],
            sublist[0],
            calldate_str,
            sublist[3],
            sublist[5].replace("@", "_"),
            "NA",
            sublist[10],
            sublist[9],
            sublist[11],
            sublist[4],
            sublist[8],
        ]

        list_for_postgres.append(temp_list)

    len_postgres = len(list_for_postgres)
    logger.info(f"Length of list for postgres - {len_postgres}")

    return list_for_postgres


def write_metadata_to_database(list_for_postgres):
    ### Create connection to database, create SQL statement, parse through list of metadata and queue to SQL, commit, and close

    # test_sublist = [['20240406060051_2222316_4098880506.mp3', '2222316', '20240406060051', '4098880506', 'U57E71T6611558F_10.125.17.149', 'NA', 'AGENT - Customer Hung Up', 'Main_Dialer_Campaign', 3179521, 'RGREBE', 17], ['20240406060137_2222315_5808535301.mp3', '2222315', '20240406060137', '5808535301', 'U57F77T661155BE_10.125.25.83', 'NA', 'AGENT - Left Message Machine', 'Main_Dialer_Campaign', 3179521, 'ASTAVROPOULOS', 1], ['20240406060157_2222313_2103460654.mp3', '2222313', '20240406060157', '2103460654', 'U58170T661155D2_10.125.17.149', 'NA', 'AGENT - CUST 8', 'Main_Dialer_Campaign', 3179521, 'RGREBE', 20], ['20240406060208_2222238_9547436546.mp3', '2222238', '20240406060208', '9547436546', 'U58225T661155DD_10.125.17.149', 'NA', 'AGENT - Left Message Machine', 'Main_Dialer_Campaign', 3179521, 'ASTAVROPOULOS', 23]]

    conn = psycopg2.connect(
        dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port
    )
    cur = conn.cursor()

    sql = "INSERT INTO livevox_test (recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    written_records = 0

    try:
        for sublist in list_for_postgres:
            cur.execute(sql, sublist)
            logger.info("Entry written to database")
            written_records = written_records + 1

        conn.commit()
        logger.info(str(written_records) + "entries written to database")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

    finally:
        # Close PostgreSQL connection
        cur.close()
        conn.close()
        logger.info("Database connection closed")


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

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )

    bucket = "livevoxpre210test"

    s3 = boto3.resource(service_name="s3", region_name="us-west-2")

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
    logger.info("Differences in s3 files compared to local:")
    print(len(differences_in_s3))
    logger.info(len(differences_in_s3))

    print("Differences in local files compared to s3: ")
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
            cleanup(tmpdir_path)
            print("File cleanup completed.")
            logger.info("File cleanup completed.")
        except Exception as e:
            print(f"An error occurred: {e}")
            logger.exception(f"An error occurred: {e}", exc_info=True)
    else:
        # Build functions to identify missing files in S3 and reupload
        print("nothing")


def folder_exists(bucket: str, path: str) -> bool:
    # Check to see if YYYY/MM/DD diretory exists within S3 bucket
    # based on first 6 characters of filename

    s3 = boto3.resource(service_name="s3", region_name="us-west-2")

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

    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
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


def cleanup(tmpdirectory):
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

        print("All files deleted successfully.")
        logger.info("All files deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.exception(f"An error occurred: {e}", exc_info=True)


def get_date():
    start_date_uf = input(
        "Please enter the date you want to download recordings for in MMDDYYYY format: "
    )

    try:
        date = datetime.datetime.strptime(start_date_uf, "%m%d%Y")
        start_of_day = datetime.datetime(date.year, date.month, date.day, 2, 0, 0)
        end_of_day = datetime.datetime(date.year, date.month, date.day + 1, 1, 59, 59)
        logger.info(f"The starting date is: {start_of_day}")
        logger.info(f"The ending date is {end_of_day}")

        start_epoch = int(start_of_day.timestamp() * 1000 + 21600000)
        logger.info(f"Start epoch time is: {start_epoch}")
        # start_epoch = int(start_of_day.timestamp() * 1000)

        end_epoch = int(end_of_day.timestamp() * 1000 + 21600000)
        logger.info(f"Ending epoch time is: {end_epoch}")
        # end_epoch = int(end_of_day.timestamp() * 1000)

        return start_epoch, end_epoch

    except ValueError:
        print("Invalid date format. Please enter date in MMDDYYYY format.")
        return None


def main():

    setup_logging()

    # GET DATE FOR DOWNLOADING AND CONVERT TO EPOCH TIME
    start_date, end_date = get_date()

    # DOWNLOAD CALL RECORDINGS
    list_for_postgres = get_call_recordings(start_date, end_date)

    # WRITE METADATA TO POSTGRES
    write_metadata_to_database(list_for_postgres)

    # UPLOAD CALL RECORDINGS
    upload_call_recordings()

    print("what")

    sys.exit(0)


if __name__ == "__main__":
    main()

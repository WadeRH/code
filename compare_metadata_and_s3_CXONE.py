import boto3
import psycopg2
from psycopg2 import Error
import csv
import json

# AWS credentials and region
json_file_path = r".secrets/amazon.json"

with open(json_file_path, "r") as f:
    aws_creds = json.load(f)

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
aws_region = 'us-west-2'

# S3 Bucket name and prefix (folder path)
bucket_name = 'cxonearchivetransfer'
prefix = ''  

# RDS PostgreSQL connection details
db_host = 'callrecordinginfo.cx8wkeu24exk.us-west-2.rds.amazonaws.com'
db_port = '5432'
db_name = 'callrecordinginfo'
db_user = 'call_recordings_user'
db_password = 'ycx$qWs@nVP4T$f4'

def get_s3_filenames(bucket_name, prefix=''):
    """
    Get a list of all file names in an S3 bucket, including subfolders.
    """
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    paginator = s3.get_paginator('list_objects_v2')

    s3_filenames = []
    for result in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in result:
            for obj in result['Contents']:
                key = obj['Key']
                if key.endswith(('.mp4', '.wav', '.mp3')) and 'SCREEN' not in key:  # Only include items ending with these file types
                    trimmed = key[8:]
                    s3_filenames.append(trimmed)
                    print(trimmed)

    s3_total = len(s3_filenames)
    
    with open("s3_filenames.csv", 'w', newline= '') as output:
        wr = csv.writer(output, dialect='excel')
        for element in s3_filenames:
            wr.writerow([element])
        output.close()
    
    return s3_filenames, s3_total

def get_database_filenames():
    """
    Get a list of filenames from the metadata table in the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_password)
        cursor = conn.cursor()

        cursor.execute("SELECT filenamenopath FROM cxone_metadata")
        rows = cursor.fetchall()

        database_filenames = [row[0] for row in rows]
        
        database_total = len(database_filenames)
        
        with open("metadata_filenames.csv", 'w', newline= '') as output:
            wr = csv.writer(output, dialect='excel')
            for element in database_filenames:
                wr.writerow([element])
            output.close()

        return database_filenames, database_total

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    
    finally:
        if conn:
            cursor.close()
            conn.close
			
			
def find_differences(list1, list2):
    """
    Find differences between two lists.
    """
    set1 = set(list1)
    set2 = set(list2)

    in_metadata_not_in_s3 = set1 - set2
    in_s3_not_in_metadata = set2 - set1

    return in_metadata_not_in_s3, in_s3_not_in_metadata

if __name__ == "__main__":
    # Get filenames from S3
    s3_filenames, s3_count = get_s3_filenames(bucket_name, prefix)

    # Get filenames from the database
    database_filenames, metadata_count = get_database_filenames()

    # Compare metadata and bucket file lists
    missing_in_s3, missing_in_metadata = find_differences(database_filenames, s3_filenames)
    
    print("Files in metadata but missing in S3:")
    for filename in missing_in_s3:
        print(filename)
        
    with open("in_meta_not_s3.csv", 'w', newline= '') as output:
        wr = csv.writer(output, dialect='excel')
        for element in missing_in_s3:
            wr.writerow([element])
        output.close()
        
    print("Files in S3 but missing in metadata:")
    for filename in missing_in_metadata:
        print(filename)
        
    with open("in_s3_not_meta.csv", 'w', newline= '') as output:
        wr = csv.writer(output, dialect='excel')
        for element in missing_in_metadata:
            wr.writerow([element])
        output.close()
        
    print ("***** Number of files in the metadata but missing from S3 = " + str(len(missing_in_s3)))
    
    print ("***** Number of files in S3 but missing from the metadata = " + str(len(missing_in_metadata)))

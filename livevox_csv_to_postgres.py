import boto3
import psycopg2
import csv
import json



# AWS credentials and region
json_file_path = r".secrets/amazon.json"

with open(json_file_path, "r") as f:
    aws_creds = json.load(f)

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
region_name = 'us-west-2'

# PostgreSQL database connection details
db_host = 'callrecordinginfo.cx8wkeu24exk.us-west-2.rds.amazonaws.com'
db_port = '5432'
db_name = 'callrecordinginfo'
db_user = 'call_recordings_user'
db_password = 'ycx$qWs@nVP4T$f4'

#Get bucket and filename from event
# S3 bucket and CSV file details
bucket_name = 'livevoxcallrecordings'
key = '2024/03/10/20240310.csv'

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

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
    s3_object = s3.get_object(Bucket=bucket_name, Key=key)
    csv_data = s3_object['Body'].read().decode('utf-8').splitlines()

    # Parse CSV data and insert into PostgreSQL table
    csv_reader = csv.reader(csv_data)
    next(csv_reader)  # Skip header row if present
    for row in csv_reader:
        # Assuming your table schema matches the CSV file columns
        cursor.execute("INSERT INTO livevox_metadata (recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
    
    # Commit the transaction
    conn.commit()
    print(key)
    print("Data imported successfully!")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    # Close PostgreSQL connection
    cursor.close()
    conn.close()

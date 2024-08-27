import boto3
import psycopg2
import csv
import json


entered_filename = input("Please enter the zip file name to process: ")

# PostgreSQL database connection details
db_host = "callrecordinginfo.cx8wkeu24exk.us-west-2.rds.amazonaws.com"
db_port = "5432"
db_name = "callrecordinginfo"
db_user = "call_recordings_user"
db_password = "ycx$qWs@nVP4T$f4"


# Initialize PostgreSQL connection
conn = psycopg2.connect(
    host=db_host, port=db_port, database=db_name, user=db_user, password=db_password
)
cursor = conn.cursor()

try:
    # Parse CSV data and insert into PostgreSQL table
    with open(entered_filename, mode="r") as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row if present
        for row in csv_reader:
            # Assuming your table schema matches the CSV file columns
            cursor.execute(
                "INSERT INTO livevox_metadata (recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
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

            # USE FOR TESTING
            # cursor.execute("INSERT INTO livevox_test (recording_filename, account_number, start_time, phone_dialed, session_id, call_result, agent_result, campaign_filename, client_id, agent_name, duration_secs) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            #     (
            #         row[0],
            #         row[1],
            #         row[2],
            #         row[3],
            #         row[4],
            #         row[5],
            #         row[6],
            #         row[7],
            #         row[8],
            #         row[9],
            #         row[10],
            #     ),
            # )

    # Commit the transaction
    conn.commit()
    print(entered_filename)
    print("Data imported successfully!")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    # Close PostgreSQL connection
    cursor.close()
    conn.close()

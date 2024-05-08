import boto3
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

def get_s3_filenames(bucket_name, prefix):
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
                    # print(trimmed)

    s3_total = len(s3_filenames)
    
    # with open("s3_filenames.csv", 'w', newline= '') as output:
    #     wr = csv.writer(output, dialect='excel')
    #     for element in s3_filenames:
    #         wr.writerow([element])
    #     output.close()
    
    return s3_filenames, s3_total


if __name__ == "__main__":
    # Get filenames from S3
    month_key = '2023-05'
    
    may_s3_filenames, may_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("May 2023 calls total: " + str(may_s3_count))
    
    
    month_key = '2023-06'
    
    june_s3_filenames, june_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("June 2023 calls total: " + str(june_s3_count))
    
    
    month_key = '2023-07'
    
    july_s3_filenames, july_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("July 2023 calls total: " + str(july_s3_count))
    
    month_key = '2023-08'
    
    aug_s3_filenames, aug_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("August 2023 calls total: " + str(aug_s3_count))
    
    month_key = '2023-09'
    
    sep_s3_filenames, sep_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("September 2023 calls total: " + str(sep_s3_count))
    
    month_key = '2023-10'
    
    oct_s3_filenames, oct_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("October 2023 calls total: " + str(oct_s3_count))
    
    month_key = '2023-11'
    
    nov_s3_filenames, nov_s3_count = get_s3_filenames(bucket_name, month_key)
    
    print("November 2023 calls total: " + str(nov_s3_count))
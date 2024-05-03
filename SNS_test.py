import boto3
import json

# AWS credentials and region
json_file_path = r".secrets/amazon.json"

with open(json_file_path, "r") as f:
    aws_creds = json.load(f)

aws_access_key_id = aws_creds["aws_access_key_id"]
aws_secret_access_key = aws_creds["aws_secret_access_key"]
aws_region = 'us-west-2'

def send_sns_notification(topic_arn, message):
    sns_client = boto3.client('sns', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)
    response = sns_client.publish(
        TopicArn=topic_arn,
        Message=message
    )
    
    print("MessageId of the published message:", response['MessageId'])

if __name__ == "__main__":
    topic_arn = 'arn:aws:sns:us-west-2:416360478487:Recording_Import_Notifications'
    message = 'IGNORE - Test notification message'
    send_sns_notification(topic_arn, message)
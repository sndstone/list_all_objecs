import boto3
import csv
import os
import uuid
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get the absolute path of the directory where the script is located
script_dir = os.path.dirname(os.path.realpath(__file__))

# Function to read credentials from JSON file
def read_credentials_from_json(file_path):
    with open(file_path, "r") as json_file:
        credentials = json.load(json_file)
        return credentials

# Function to ask yes/no questions and validate the input
def ask_yes_no(question):
    while True:
        answer = input(question).lower()
        if answer in ['y', 'yes', 'n', 'no']:
            return answer in ['y', 'yes']
        else:
            print("Please answer with 'yes' or 'no' (or 'y' or 'n').")

# Asks inputs to run run the script
JSON_IMPORT = ask_yes_no("Do you want to import JSON file for configuration? (yes/no): ")

if JSON_IMPORT:
    JSON_FILE_PATH = input("Enter the JSON file path: ")
    credentials = read_credentials_from_json(JSON_FILE_PATH)
    BUCKET_NAME = credentials["bucket_name"]
    S3_ENDPOINT_URL = credentials["s3_endpoint_url"]
    AWS_ACCESS_KEY_ID = credentials["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = credentials["aws_secret_access_key"]
else:
    BUCKET_NAME = input("Enter the bucket name: ")
    S3_ENDPOINT_URL = input("Enter the S3 endpoint URL, (EXAMPLE http://example.com:443): ")
    AWS_ACCESS_KEY_ID = input("Enter the AWS access key ID: ")
    AWS_SECRET_ACCESS_KEY = input("Enter the AWS secret access key: ")

logging.info('Creating S3 client...')

# Create an S3 client
s3 = boto3.client("s3",
                  verify=False,
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  endpoint_url=S3_ENDPOINT_URL)

logging.info('S3 client created.')

logging.info('Listing objects in bucket...')

# Use the list_objects_v2 method to get a list of all objects in the bucket
response = s3.list_objects_v2(Bucket=BUCKET_NAME)

logging.info('Objects listed.')

logging.info('Writing object listing to CSV file...')

# Create and open the CSV file for object listing
with open(os.path.join(script_dir, 'object_listing.csv'), 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Key", "Size", "Last Modified"])

    # Check if the bucket contains any objects
    if 'Contents' in response:
        # Loop through each object in the bucket
        for obj in response['Contents']:
            # Add the object's key (name), size, and last modified date to the CSV file
            writer.writerow([obj['Key'], obj['Size'], obj['LastModified']])

logging.info('Object listing written to CSV file.')

print("Object listing has been exported to " + os.path.join(script_dir, 'object_listing.csv'))

HEAD_OBJECTS = ask_yes_no("Do you want to run head on all objects? (yes/no): ")

if HEAD_OBJECTS:
    logging.info('Running head operation on all objects...')
    
    # Create and open the CSV file for object heads
    with open(os.path.join(script_dir, 'object_heads.csv'), 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Key", "Exists", "Response Code"])

        # Loop through each object in the bucket
        for obj in response['Contents']:
            try:
                # Run a head operation on the object
                s3.head_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                # If the head operation is successful, write "True" to the CSV file
                writer.writerow([obj['Key'], True, None])
            except Exception as e:
                # If the head operation fails, write the response code to the CSV file
                writer.writerow([obj['Key'], False, e.response['Error']['Code']])

    logging.info('Head operation completed. Results written to CSV file.')

    print("Object heads have been exported to " + os.path.join(script_dir, 'object_heads.csv'))

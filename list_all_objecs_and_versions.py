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

logging.info('Listing object versions in bucket...')

# Use the list_object_versions method to get a list of all object versions in the bucket
response = s3.list_object_versions(Bucket=BUCKET_NAME)

logging.info('Object versions listed.')

logging.info('Writing object version listing to CSV file...')

# Create and open the CSV file for object version listing
with open(os.path.join(script_dir, 'object_version_listing.csv'), 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["Key", "VersionId", "Size", "Last Modified", "Is Latest"])

    # Check if the bucket contains any objects
    if 'Versions' in response:
        # Loop through each object version in the bucket
        for version in response['Versions']:
            # Add the object's key (name), version ID, size, last modified date, and whether it is the latest version to the CSV file
            writer.writerow([version['Key'], version['VersionId'], version['Size'], version['LastModified'], version['IsLatest']])

logging.info('Object version listing written to CSV file.')

print("Object version listing has been exported to " + os.path.join(script_dir, 'object_version_listing.csv'))

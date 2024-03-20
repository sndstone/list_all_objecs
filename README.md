S3 Object Listing and Head Operation Script
This script provides a way to list all objects in an S3 bucket and optionally perform a head operation on each object using the Boto3 Python library. The script logs all operations and exports the results to CSV files.

Prerequisites:
Python 3.6 or higher
Boto3 library installed (pip install boto3)

How to use:
Clone the repository.
Run the script python s3_object_listing.py.
When prompted, enter the necessary information:
If you want to import a JSON file for configuration, enter "yes" and provide the path to the JSON file. The JSON file should have the following structure:

{
    "bucket_name": "<your_bucket_name>",
    "s3_endpoint_url": "<your_s3_endpoint_url>",
    "aws_access_key_id": "<your_aws_access_key_id>",
    "aws_secret_access_key": "<your_aws_secret_access_key>"
}

If you don't want to use a JSON file, enter "no" and provide the necessary information when prompted.
After the object listing has been exported, you will be asked if you want to run a head operation on all objects. If you answer with 'yes', the script will perform a head operation on each object and export the results to a CSV file.
Output
The script creates two CSV files in the same directory as the script:

object_listing.csv: Contains a list of all objects in the bucket, including each object's key (name), size, and the date it was last modified.
object_heads.csv: Contains the results of the head operations on each object, including each object's key (name), whether the operation was successful, and the response code if it was not.
Logging
The script logs all the operations to the console. The log level is set to INFO.

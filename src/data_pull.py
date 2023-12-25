import requests
import json
import csv
from datetime import datetime as dt
import base64
import boto3
from botocore.exceptions import NoCredentialsError
import snowflake.connector
import pandas as pd
from io import StringIO
from boto3.s3.transfer import S3Transfer
import logging
import os
from dotenv import load_dotenv
from utilities import *  # Import the send_email function from utilities.py

# Set up the email content
subject = "Impact Log File"
body = "Please find attached the log file."

# # Configure logging to a file in a 'logs' directory
# log_directory = os.path.abspath(os.path.join(__file__, "../..", "logs"))
# os.makedirs(log_directory, exist_ok=True)
# log_file_path = os.path.join(log_directory, "impact_log.log")
# # Set up logging
# log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "logs"))
# file_handler = logging.FileHandler(log_file_path)

# # Add a formatter and a file handler to the logger
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_file_path, mode='a')])
# logger = logging.getLogger(__name__)
# logger.addHandler(file_handler)

#########################################################################################################
# Error Logging
#########################################################################################################
path = os.path.abspath(os.path.join(__file__ ,"../.."))+'/logs'

with open(os.path.join(path,'impact_log.log'), 'w') as logfile:
    pass
logging.basicConfig(filename=f'{path}/impact_log.log', level=logging.INFO, 
                format='%(asctime)s %(levelname)s %(name)s %(message)s')

logger=logging.getLogger(__name__)     

file_ts = datetime.datetime.now().strftime("%y%m%d%H%M%S")

logger.info(f'Impact report data load begins at {str(datetime.datetime.now())}...')

file_path = os.path.abspath(os.path.join(__file__ ,"../.."))
env_path = f'{file_path}/src/.env'
load_dotenv(dotenv_path=env_path)

# Email configuration
sender_email = "salma@seed.com"  # Your Gmail Enterprise email address
app_password = os.environ.get('app_password')  # If you have two-factor authentication enabled
recipient_email = ["salma@seed.com", "data-analytics@seed.com"]  # Email address to send the log file to

end_date = dt.now().strftime('%Y-%m-%d')
file_ts = dt.now().strftime('%Y-%m-%d_%H:%M:%S')
# Your S3 bucket name
s3_bucket_name = 'impact-performance-data'
s3_file_name = f'performance_{file_ts}.csv'

# Your AWS credentials for S3
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
# Retrieve Snowflake credentials from Heroku environment variables
snowflake_user = os.environ.get('SNOWFLAKE_USER')
snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')
def update_snowflake_table(data):
    # conn = None  # Initialize conn to None outside try block
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            role='ACCOUNTADMIN',
            account='xca53965',
            warehouse='QUERY_EXECUTION',
            database='STAGE_DB',
            schema='GROWTH'
        )

        # Convert data to a Pandas DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        # Create a CSV in-memory buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Seek to the beginning of the buffer
        csv_buffer.seek(0)


        # Use COPY command to load data into Snowflake table
        cursor = conn.cursor()
        cursor.execute(f"""
                       USE DATABASE STAGE_DB;""")
        cursor.execute("USE SCHEMA GROWTH;")
        cursor.execute("TRUNCATE TABLE PERFORMANCE_DATA;""")
        cursor.execute(f"""
                        COPY INTO "PERFORMANCE_DATA"
                        FROM @my_stage/{s3_file_name}
                        FILE_FORMAT = (
                            TYPE=CSV,
                            SKIP_HEADER=1,
                            FIELD_OPTIONALLY_ENCLOSED_BY='"', -- Specify the double quote as the field enclosure
                            FIELD_DELIMITER=',',
                            TRIM_SPACE=TRUE,
                            REPLACE_INVALID_CHARACTERS=TRUE,
                            DATE_FORMAT=AUTO,
                            TIME_FORMAT=AUTO,
                            TIMESTAMP_FORMAT=AUTO
                        )
                        ON_ERROR=CONTINUE
                        PURGE=TRUE;""")

        logger.info("Data loaded into Snowflake table successfully.")
    except Exception as e:
        logger.error(f"Error updating Snowflake table: {e}")
    # finally:
    #     if conn is not None:
    #         conn.close()  # Close conn if it is not None

def fetch_and_filter_data():
    account_sid = os.environ.get('account_sid')
    auth_token = os.environ.get('auth_token')

    end_date = dt.now().strftime('%Y-%m-%d')

    base_url = f'https://IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1:uFNo8XDUvvQYd6RSDmGzCievx%7ENDYB%7EB@api.impact.com/Advertisers/IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1/ReportExport/att_adv_performance_by_day_pm_only.json?START_DATE=2023-07-01&END_DATE={end_date}&SUBAID=19848'
    headers = {
        'Accept': 'text/csv',
        'Authorization': 'Basic ' + base64.b64encode(f"{account_sid}:{auth_token}".encode('utf-8')).decode('utf-8')
    }

    # Send the initial request to get the job status and download URL
    response = requests.get(base_url, headers=headers)

    if response.status_code == 200:
        try:
            # Parse the JSON response
            response_data = json.loads(response.text)

            # Extract the ResultUri
            result_uri = response_data.get('ResultUri', '')

            if result_uri:
                # Construct the download URL using the ResultUri
                download_url = f'https://api.impact.com{result_uri}'
                logger.info(f'The dowbload url is: {download_url}')
                # Download the CSV file
                response_csv = requests.get(download_url, headers=headers)

                # Use the csv module to parse the CSV response
                csv_reader = csv.reader(response_csv.text.splitlines())

                # Assuming the first row is the header
                header = next(csv_reader)
                # print("Available columns:", header)  # Print the actual header names

                # Get the indices of the desired columns dynamically
                desired_columns = ['date_display', 'media_count', 'Clicks', 'Actions', 'Revenue', 'ActionCost', 'OtherCost', 'TotalCost', 'CPC']
                indices = [header.index(col) if col in header else None for col in desired_columns]

                # Initialize the filtered data list with the header
                filtered_data = [desired_columns]

                # Iterate over rows and extract desired columns
                for row in csv_reader:
                    filtered_row = [row[i] if i is not None else None for i in indices]
                    filtered_data.append(filtered_row)

                return filtered_data

            else:
                logger.error('ResultUri not found in the response.')
                return "ResultUri not found in the response."

        except (json.JSONDecodeError, csv.Error) as e:
            logger.error(f'Error: {e}')
            return f"Error: {e}"

    else:
        logger.error(f"Error: {response.status_code}\nResponse content: {response.text}")
        return f"Error: {response.status_code}\nResponse content: {response.text}"
        

def save_to_csv(data, filename=f'performance_{end_date}.csv'):
    with open(filename, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(data)

# Fetch and filter data
filtered_data_result = fetch_and_filter_data()

# Function to upload a file to S3
def upload_to_s3(data, s3_file_name):
    try:
        # Connect to S3
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        transfer = S3Transfer(s3)

        # Convert data to a Pandas DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        # Create a CSV in-memory buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Seek to the beginning of the buffer
        csv_buffer.seek(0)

        # Use put_object to upload the CSV file
        s3.put_object(Body=csv_buffer.getvalue().encode('utf-8'), Bucket=s3_bucket_name, Key=s3_file_name)


        logger.info(f"File uploaded successfully to {s3_bucket_name}/{s3_file_name}")

    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")


# Fetch and filter data
filtered_data_result = fetch_and_filter_data()

# Upload the filtered data to S3 as a CSV file

upload_to_s3(filtered_data_result, s3_file_name)
update_snowflake_table(filtered_data_result)

count = email_wrapper(file_path, recipient_email)
                                      
if count <= 0:                
    send_email(file_path, sender_email, recipient_email, subject, app_password)  
else:
    send_failure_email(file_path, sender_email, recipient_email, subject, app_password) 
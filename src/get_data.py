import requests
import json
import csv
from datetime import datetime as dt
from datetime import timedelta
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
import time
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

account_sid = os.environ.get('account_sid')
auth_token = os.environ.get('auth_token')

base_url = f'https://IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1:uFNo8XDUvvQYd6RSDmGzCievx%7ENDYB%7EB@api.impact.com/Advertisers/IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1/ReportExport/att_adv_performance_by_day_pm_only.json?START_DATE=2023-07-01&END_DATE={end_date}&SUBAID=19848'
headers = {
    'Accept': 'text/csv',
    'Authorization': 'Basic ' + base64.b64encode(f"{account_sid}:{auth_token}".encode('utf-8')).decode('utf-8')
    }


def update_snowflake_table_2(data):
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

        # Use COPY command to load data into Snowflake stage table
        stage_table_name = 'PERFORMANCE_DATA'
        stage_file_name = 'my_stage_file.csv'

        cursor = conn.cursor()
        cursor.execute(f"""
                       USE DATABASE STAGE_DB;""")
        cursor.execute("USE SCHEMA GROWTH;")

        # Upload data to the Snowflake stage table
        cursor.execute(f"""
                        PUT file://{csv_buffer}
                        @mystage/{s3_file_name}""")

        # Use MERGE to insert new records and update existing ones
        cursor.execute(f"""
                        MERGE INTO PERFORMANCE_DATA target
                        USING {stage_table_name}@my_stage source
                        ON target.display_date = source.display_date
                        WHEN MATCHED THEN
                            UPDATE SET
                                target.display_date = source.display_date,
                                target.media_count = source.media_count,
                                target.clicks = source.clicks,
                                target.actions = source.actions,
                                target.revenue = source.revenue,
                                target.actioncost = source.actioncost,
                                target.othercost = source.othercost,
                                target.totalcost = source.totalcost,
                                target.cpc = source.cpc
                        WHEN NOT MATCHED THEN
                            INSERT (display_date, media_count, clicks, actions, 
                            revenue, actioncost, othercost, totalcost, cpc)
                            VALUES (source.display_date, source.media_count, source.clicks, source.actions, 
                            source.revenue, source.actioncost, source.othercost, source.totalcost, source.cpc);""")

        logger.info("Data merged into Snowflake table successfully.")
    except Exception as e:
        logger.error(f"Error updating Snowflake table: {e}")

# ... (Other functions remain unchanged)

def update_snowflake_table(data, db, stage_name):
    # conn = None  # Initialize conn to None outside try block
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            role='ACCOUNTADMIN',
            account='xca53965',
            warehouse='QUERY_EXECUTION',
            database=f'{db}',
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
                       USE DATABASE {db};""")
        cursor.execute("USE SCHEMA GROWTH;")
        cursor.execute("TRUNCATE TABLE PERFORMANCE_DATA;""")
        cursor.execute(f"""
                        COPY INTO "PERFORMANCE_DATA"
                        FROM {stage_name}/{s3_file_name}
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
                        PURGE=FALSE;""")

        logger.info("Data loaded into Snowflake table successfully.")
    except Exception as e:
        logger.error(f"Error updating Snowflake table: {e}")
    # finally:
    #     if conn is not None:
    #         conn.close()  # Close conn if it is not None

# def fetch_and_filter_data():


    # Send the initial request to get the job status and download URL
    # response = requests.get(base_url, headers=headers)

    # if response.status_code == 200:
    #     try:
    #         # Parse the JSON response
    #         response_data = json.loads(response.text)

    #         # Extract the ResultUri
    #         result_uri = response_data.get('ResultUri', '')

    #         if result_uri:
    #             # Construct the download URL using the ResultUri
    #             download_url = f'https://api.impact.com{result_uri}'
    #             logger.info(f'The dowbload url is: {download_url}')
    #             # Download the CSV file
    #             response_csv = requests.get(download_url, headers=headers)

    #             # Use the csv module to parse the CSV response
    #             csv_reader = csv.reader(response_csv.text.splitlines())

    #             # Assuming the first row is the header
    #             header = next(csv_reader)
    #             # print("Available columns:", header)  # Print the actual header names

    #             # Get the indices of the desired columns dynamically
    #             desired_columns = ['date_display', 'media_count', 'Clicks', 'Actions', 'Revenue', 'ActionCost', 'OtherCost', 'TotalCost', 'CPC']
    #             indices = [header.index(col) if col in header else None for col in desired_columns]

    #             # Initialize the filtered data list with the header
    #             filtered_data = [desired_columns]

    #             # Iterate over rows and extract desired columns
    #             for row in csv_reader:
    #                 filtered_row = [row[i] if i is not None else None for i in indices]
    #                 filtered_data.append(filtered_row)

    #             return filtered_data

    #         else:
    #             logger.error('ResultUri not found in the response.')
    #             return "ResultUri not found in the response."

    #     except (json.JSONDecodeError, csv.Error) as e:
    #         logger.error(f'Error: {e}')
    #         return f"Error: {e}"

    # else:
    #     logger.error(f"Error: {response.status_code}\nResponse content: {response.text}")
    #     return f"Error: {response.status_code}\nResponse content: {response.text}"
        

# def save_to_csv(data, filename=f'performance_{end_date}.csv'):
    # with open(filename, 'w', newline='') as csvfile:
    #     csv_writer = csv.writer(csvfile)
    #     csv_writer.writerows(data)

# Fetch and filter data
# filtered_data_result = fetch_and_filter_data()

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


def get_replay_uri(retry_on_empty_row=True):
    # Update the base_url to fetch the ReplayUri
    base_url = f'https://IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1:uFNo8XDUvvQYd6RSDmGzCievx%7ENDYB%7EB@api.impact.com/Advertisers/IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1/ReportExport/att_adv_performance_by_day_pm_only.json?START_DATE=2023-07-01&END_DATE={end_date}&SUBAID=19848'
    headers = {
        'Accept': 'application/json',  # Update Accept header to expect JSON response
        'Authorization': 'Basic ' + base64.b64encode(f"{account_sid}:{auth_token}".encode('utf-8')).decode('utf-8')
    }

    retry_count = 0
    max_retries = 5  # You can adjust the maximum number of retries as needed

    while retry_count < max_retries:
        response = requests.get(base_url, headers=headers)

        if response.status_code == 200:
            try:
                # Parse the JSON response
                response_data = json.loads(response.text)
                replay_uri = response_data.get('ReplayUri', '')

                if replay_uri:
                    return replay_uri
                else:
                    logger.error('ReplayUri not found in the response.')
                    return None

            except json.JSONDecodeError as e:
                logger.error(f'Error decoding JSON: {e}')
                return None

        elif response.status_code == 406 and retry_on_empty_row:
            # Check if the response has an empty row
            logger.warning('Empty row found in the response. Retrying after 1 minute...')
            time.sleep(60)  # Wait for 1 minute before retrying
            retry_count += 1

        else:
            logger.error(f"Error: {response.status_code}\nResponse content: {response.text}")
            return None

    logger.error(f"Exceeded maximum retries ({max_retries}). Unable to obtain ReplayUri.")
    return None


def get_result_uri(replay_uri):
    # Use the PUT method to get the ResultUri from the ReplayUri
    put_url = f'https://api.impact.com{replay_uri}'
    response_put = requests.put(put_url, headers=headers)

    if response_put.status_code == 200:
        try:
            response_data_put = json.loads(response_put.text)
            result_uri = response_data_put.get('ResultUri', '')

            if result_uri:
                return result_uri
            else:
                logger.error('ResultUri not found in the PUT response.')
                return None

        except json.JSONDecodeError as e:
            logger.error(f'Error decoding PUT response JSON: {e}')
            return None

    else:
        logger.error(f"Error in PUT request: {response_put.status_code}\nResponse content: {response_put.text}")
        return None
    
# Fetch the ReplayUri
replay_uri = get_replay_uri()

if replay_uri:
    # Use the ReplayUri to get the ResultUri
    result_uri = get_result_uri(replay_uri)

    if result_uri:
        # Continue with the existing logic for downloading CSV, processing data, and updating Snowflake
        # Fetch and filter data
        download_url = f'https://api.impact.com{result_uri}'
        logger.info(f'The download URL is: {download_url}')

        retry_count_csv = 0
        max_retries_csv = 5  # You can adjust the maximum number of retries as needed
        filtered_data = None  # Initialize filtered_data outside the loop

        while retry_count_csv < max_retries_csv:
            # Download the CSV file with 'Accept' header set to CSV
            response_csv = requests.get(download_url, headers={'Accept': 'text/csv', **headers})

            if response_csv.status_code == 200:
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
                    # Transform the "date_display" column format
                    date_display_str = row[indices[0]]
                    if date_display_str:
                        # Convert to datetime and then format to 'yyyy-mm-dd'
                        date_obj = dt.strptime(date_display_str, '%b %d, %Y')
                        formatted_date = date_obj.strftime('%Y-%m-%d')
                        row[indices[0]] = formatted_date

                    filtered_row = [row[i] if i is not None else None for i in indices]
                    filtered_data.append(filtered_row)

                # Upload to S3
                upload_to_s3(filtered_data, s3_file_name)

                # Update Snowflake table
                update_snowflake_table(filtered_data, 'STAGE_DB', '@my_stage')
                update_snowflake_table(filtered_data, 'PROD_DB', '@impact_stg')

                # Exit the loop since CSV download and processing were successful
                break

            elif response_csv.status_code == 406:
                # Check if the response has an empty row
                logger.warning('Empty row found in the CSV response. Retrying after 1 minute...')

                time.sleep(60)  # Wait for 1 minute before retrying
                retry_count_csv += 1
            else:
                logger.error(f"Error in CSV download: {response_csv.status_code}\nResponse content: {response_csv.text}")
                break  # Exit the loop in case of other errors
        else:
            logger.error(f"Exceeded maximum retries ({max_retries_csv}). Unable to obtain CSV data.")

    else:
        logger.error('Failed to obtain ResultUri from ReplayUri.')

else:
    logger.error('Failed to obtain ReplayUri.')




count = email_wrapper(file_path, recipient_email)
                                      
if count > 0:                
    # send_email(file_path, sender_email, recipient_email, subject, app_password)  
# else:
    send_failure_email(file_path, sender_email, recipient_email, subject, app_password) 
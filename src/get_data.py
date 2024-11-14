from calendar import c
from re import S
import requests
import json
import csv
from datetime import datetime as dt
import base64
import boto3
from botocore.exceptions import NoCredentialsError
import snowflake.connector
import pandas as pd
from io import StringIO, BytesIO
from boto3.s3.transfer import S3Transfer
import logging
import os
from dotenv import load_dotenv
import time
from utilities import *

# Constants and Configuration
SUBJECT = "Impact Log File"
BODY = "Please find attached the log file."
SENDER_EMAIL = "salma@seed.com"
RECIPIENT_EMAIL = ["salma@seed.com"]
END_DATE = dt.now().strftime("%Y-%m-%d")
FILE_TS = dt.now().strftime("%Y-%m-%d_%H:%M:%S")
S3_BUCKET_NAME = "impact-performance"
S3_FILE_NAME = f"performance_{FILE_TS}.csv"
DB_NAME = os.environ.get("DB_NAME")
STAGE_NAME = "@impact_stg"
FILE_PATH = os.path.abspath(os.path.join(__file__, "../.."))
LOG_DIRECTORY = f"{FILE_PATH}/logs"
LOG_FILE = "impact_log"
LOGGING_HEADER = "IMPACT DATA LOAD"
LOG_FILE_PATH = os.path.join(LOG_DIRECTORY, "impact_log.log")
ACCOUNT_SID = os.environ.get("account_sid")
AUTH_TOKEN = os.environ.get("auth_token")
SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL")
BASE_URL = f"https://IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1:uFNo8XDUvvQYd6RSDmGzCievx%7ENDYB%7EB@api.impact.com/Advertisers/IRgqMP5TEkmE4304993FGxhxf6x2xHBsb1/ReportExport/att_adv_performance_by_day_pm_only.json?START_DATE=2023-07-01&END_DATE={END_DATE}&SUBAID=19848"
HEADERS = {
    "Accept": "text/csv",
    "Authorization": "Basic "
    + base64.b64encode(f"{ACCOUNT_SID}:{AUTH_TOKEN}".encode("utf-8")).decode("utf-8"),
}

# Initialize logging
os.makedirs(LOG_DIRECTORY, exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def load_environment_variables():
    file_path = os.path.abspath(os.path.join(__file__, "../.."))
    env_path = f"{file_path}/src/.env"
    load_dotenv(dotenv_path=env_path)


def setup_aws_credentials():
    return {
        "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
    }


def setup_snowflake_credentials():
    return {
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "role": "ACCOUNTADMIN",
        "account": "xca53965",
        "warehouse": "QUERY_EXECUTION",
        "database": DB_NAME,
        "schema": "GROWTH",
    }


def connect_to_snowflake(credentials):
    return snowflake.connector.connect(**credentials)


def fetch_replay_uri():
    response = requests.get(BASE_URL, headers=HEADERS)
    if response.status_code == 200:
        try:
            response_data = response.json()
            replay_uri = response_data.get("ReplayUri", "")
            if replay_uri:
                return replay_uri
            logger.error("ReplayUri not found in the response.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            slack_notification(
                SLACK_CHANNEL, SLACK_API_TOKEN, f"Error decoding JSON: {e}")
    else:
        logger.error(
            f"Error: {response.status_code}\nResponse content: {response.text}")
        slack_notification(
            SLACK_CHANNEL,
            SLACK_API_TOKEN,
            f"Error: {response.status_code}\nResponse content: {response.text}",
        )
    return None


def fetch_result_uri(replay_uri):
    put_url = f"https://api.impact.com{replay_uri}"
    response_put = requests.put(put_url, headers=HEADERS)
    if response_put.status_code == 200:
        try:
            response_data_put = response_put.json()
            result_uri = response_data_put.get("ResultUri", "")
            if result_uri:
                return result_uri
            logger.error("ResultUri not found in the PUT response.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding PUT response JSON: {e}")
            slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f"Error decoding PUT response JSON: {e}")
    else:
        logger.error(
            f"Error in PUT request: {response_put.status_code}\nResponse content: {response_put.text}"
        )
        slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f"Error in PUT request: {response_put.status_code}\nResponse content: {response_put.text}")
    return None


def download_and_process_csv(result_uri):
    download_url = f"https://api.impact.com{result_uri}"
    logger.info(f"The download URL is: {download_url}")
    retry_count_csv = 0
    max_retries_csv = 5

    while retry_count_csv < max_retries_csv:
        response_csv = requests.get(
            download_url, headers={"Accept": "text/csv", **HEADERS}
        )
        if response_csv.status_code == 200:
            csv_reader = csv.reader(response_csv.text.splitlines())
            header = next(csv_reader)
            desired_columns = [
                "date_display",
                "media_count",
                "Clicks",
                "Actions",
                "Revenue",
                "ActionCost",
                "OtherCost",
                "TotalCost",
                "CPC",
            ]
            indices = [
                header.index(col) if col in header else None for col in desired_columns
            ]
            filtered_data = [desired_columns]
            for row in csv_reader:
                date_display_str = row[indices[0]]
                if date_display_str:
                    date_obj = dt.strptime(date_display_str, "%b %d, %Y")
                    row[indices[0]] = date_obj.strftime("%Y-%m-%d")
                filtered_row = [row[i] if i is not None else None for i in indices]
                filtered_data.append(filtered_row)
            return filtered_data
        elif response_csv.status_code == 406:
            logger.warning(
                "Empty row found in the CSV response. Retrying after 1 minute..."
            )
            time.sleep(60)
            retry_count_csv += 1
        else:
            logger.error(
                f"Error in CSV download: {response_csv.status_code}\nResponse content: {response_csv.text}"
            )
            slack_notification(
                SLACK_CHANNEL,
                SLACK_API_TOKEN,
                f"Error in CSV download: {response_csv.status_code}\nResponse content: {response_csv.text}",
            )
            break
    else:
        logger.error(
            f"Exceeded maximum retries ({max_retries_csv}). Unable to obtain CSV data."
        )
        slack_notification(
            SLACK_CHANNEL,
            SLACK_API_TOKEN,
            f"Exceeded maximum retries ({max_retries_csv}). Unable to obtain CSV data.",
        )
    return None


def upload_to_s3(data, s3_file_name, aws_credentials):
    try:
        s3 = boto3.client("s3", **aws_credentials)
        transfer = S3Transfer(s3)
        df = pd.DataFrame(data[1:], columns=data[0])
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        s3.put_object(
            Body=csv_buffer.getvalue().encode("utf-8"),
            Bucket=S3_BUCKET_NAME,
            Key=s3_file_name,
        )
        logger.info(f"File uploaded successfully to {S3_BUCKET_NAME}/{s3_file_name}")
    except Exception as e:
        logger.error(f"Error uploading file to S3: {e}")
    slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f"File uploaded successfully to {S3_BUCKET_NAME}/{s3_file_name}")


def update_snowflake_table(data, credentials, stage_name):
    try:
        conn = connect_to_snowflake(credentials)
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {DB_NAME};")
        cursor.execute("USE SCHEMA GROWTH;")
        # cursor.execute("TRUNCATE TABLE PERFORMANCE_DATA;")
        # Initialize S3 client
        s3_client = boto3.client('s3')

        # List all files in the specified S3 bucket and prefix
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=S3_FILE_NAME)
        files = response.get('Contents', [])

        df = pd.DataFrame(data[1:], columns=data[0])
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        cursor.execute(
            f"""
            COPY INTO "PERFORMANCE_DATA"
            FROM {stage_name}/{S3_FILE_NAME}
            FILE_FORMAT = (
                TYPE=CSV,
                SKIP_HEADER=1,
                FIELD_OPTIONALLY_ENCLOSED_BY='"',
                FIELD_DELIMITER=',',
                TRIM_SPACE=TRUE,
                REPLACE_INVALID_CHARACTERS=TRUE,
                DATE_FORMAT=AUTO,
                TIME_FORMAT=AUTO,
                TIMESTAMP_FORMAT=AUTO
            )
            ON_ERROR=CONTINUE
            PURGE=FALSE;
        """
        )
        logger.info("Data loaded into Snowflake table successfully.")
    except Exception as e:
        logger.error(f"Error updating Snowflake table: {e}")
        slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f"Error updating Snowflake table: {e}")

def update_snowflake_table_from_s3(credentials):
    try:
        conn = connect_to_snowflake(credentials)
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {DB_NAME};")
        cursor.execute("USE SCHEMA GROWTH;")
        # cursor.execute("TRUNCATE TABLE PERFORMANCE_DATA;")
        # Initialize S3 client
        # s3_client = boto3.client('s3')

        # # List all files in the specified S3 bucket and prefix
        # response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        # files = response.get('Contents', [])

        # for file in files:
        #     file_key = file['Key']

        #     # Skip any "folders" or empty files
        #     if file_key.endswith('/') or file['Size'] == 0:
        #         continue

        #     # Read the file from S3
        #     obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        #     data = obj['Body'].read()

        #     # Convert data to a DataFrame (assuming CSV format)
        #     df = pd.read_csv(BytesIO(data))

        #     # Define the Snowflake stage to use (or create a temporary one)
        stage_name = "PROD_DB.GROWTH.IMPACT_CLEAN"
        # cursor.execute(f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' FIELD_DELIMITER = ',' SKIP_HEADER = 1 TRIM_SPACE=TRUE REPLACE_INVALID_CHARACTERS=TRUE DATE_FORMAT=AUTO TIME_FORMAT=AUTO TIMESTAMP_FORMAT=AUTO);")
        # Step 2: Initialize S3 client
        # s3_client = boto3.client("s3")
        #     cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

        #     # Save DataFrame to Snowflake (uploading to the stage first)
        #     csv_data = df.to_csv(index=False, header=False)
        #     cursor.execute(f"PUT 'file://{file_key}' @{stage_name}")
        # Step 3: List all files in the specified S3 bucket
        # response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
        # files = response.get("Contents", [])

        file_list = cursor.execute(f"LIST @{stage_name}").fetchall()

        for file in file_list:
            file_key = file[0].split('/')[-1]
            file_date_str = file_key.split('_')[1]  # Extract date part
            if file_date_str == dt.now().strftime("%Y-%m-%d"):

                # Step 4: Loop through all files in the S3 bucket
                # for file in files:
                #     file_key = file["Key"]

                #     # Skip folders or empty files
                #     if file_key.endswith("/") or file["Size"] == 0:
                #         continue
                #     # Step 5: Upload files to the Snowflake internal stage
                #     # Here we are putting files from S3 into the Snowflake stage
                #     s3_url = f"s3://{S3_BUCKET_NAME}/{file_key}"
                #     cursor.execute(f"PUT 'file://{s3_url}' @{stage_name}")

                # Step 6: Use the COPY INTO command to load data from the stage into the table
                # Step 1: Load data into a temporary staging table with file_key column added
                cursor.execute("""CREATE OR REPLACE TEMPORARY TABLE PERFORMANCE_DATA_TEMP (
                    DATE_DISPLAY VARCHAR(16777216),
                    MEDIA_COUNT NUMBER(38,0),
                    CLICKS NUMBER(38,0),
                    ACTIONS NUMBER(38,0),
                    REVENUE FLOAT,
                    ACTIONCOST FLOAT,
                    OTHERCOST FLOAT,
                    TOTALCOST FLOAT,
                    CPC FLOAT,
                    LOADED_FILE_NAME VARCHAR(16777216) DEFAULT '' );
                    """)
                cursor.execute(
                    f"""
                    COPY INTO PERFORMANCE_DATA_TEMP
                    (
                        DATE_DISPLAY,
                        MEDIA_COUNT,
                        CLICKS,
                        ACTIONS,
                        REVENUE,
                        ACTIONCOST,
                        OTHERCOST,
                        TOTALCOST,
                        CPC,
                        LOADED_FILE_NAME
                    )
                    FROM (SELECT 
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, '{file_key}'
                        FROM @{stage_name}/{file_key})
                    FILE_FORMAT = (TYPE = 'CSV' 
                                FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
                                FIELD_DELIMITER = ',', 
                                SKIP_HEADER = 1,
                                TRIM_SPACE=TRUE,
                                REPLACE_INVALID_CHARACTERS=TRUE,
                                DATE_FORMAT=AUTO,
                                TIME_FORMAT=AUTO,
                                TIMESTAMP_FORMAT=AUTO)
                    ON_ERROR = 'CONTINUE';
                """
                )
                # Step 6: Update Existing Records (using the DATE_DISPLAY column)
                update_query = f"""
                    UPDATE PROD_DB.GROWTH.PERFORMANCE_DATA p
                    SET
                        p.MEDIA_COUNT = s.MEDIA_COUNT,
                        p.CLICKS = s.CLICKS,
                        p.ACTIONS = s.ACTIONS,
                        p.REVENUE = s.REVENUE,
                        p.ACTIONCOST = s.ACTIONCOST,
                        p.OTHERCOST = s.OTHERCOST,
                        p.TOTALCOST = s.TOTALCOST,
                        p.CPC = s.CPC,
                        p.LOADED_FILE_NAME = s.LOADED_FILE_NAME
                    FROM
                        PERFORMANCE_DATA_TEMP s
                    WHERE
                        p.DATE_DISPLAY = s.DATE_DISPLAY;
                """
                cursor.execute(update_query)

                # Step 7: Insert New Records (those not already in the table)
                insert_query = f"""
                    INSERT INTO PROD_DB.GROWTH.PERFORMANCE_DATA (
                        DATE_DISPLAY,
                        MEDIA_COUNT,
                        CLICKS,
                        ACTIONS,
                        REVENUE,
                        ACTIONCOST,
                        OTHERCOST,
                        TOTALCOST,
                        CPC,
                        LOADED_FILE_NAME
                    )
                    SELECT
                        s.DATE_DISPLAY,
                        s.MEDIA_COUNT,
                        s.CLICKS,
                        s.ACTIONS,
                        s.REVENUE,
                        s.ACTIONCOST,
                        s.OTHERCOST,
                        s.TOTALCOST,
                        s.CPC,
                        LOADED_FILE_NAME
                    FROM
                        PERFORMANCE_DATA_TEMP s
                    WHERE
                        NOT EXISTS (
                            SELECT 1
                            FROM PROD_DB.GROWTH.PERFORMANCE_DATA p
                            WHERE p.DATE_DISPLAY = s.DATE_DISPLAY
                        );
                """
                cursor.execute(insert_query)
           
            # Step 8: Clean up and close the connection

    except Exception as e:
        logger.error(f"Error updating Snowflake table: {e}")
        slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, f"Error updating Snowflake table: {e}")

def main():
    logger = setup_logging(LOG_FILE)
    logger.info(f"{LOGGING_HEADER} report data load begins at {str(dt.now())}...")
    logger.info(f"Impact report data load begins at {str(dt.now())}...")
    load_environment_variables()
    aws_credentials = setup_aws_credentials()
    snowflake_credentials = setup_snowflake_credentials()
    replay_uri = fetch_replay_uri()
    if replay_uri:
        result_uri = fetch_result_uri(replay_uri)
        if result_uri:
            filtered_data = download_and_process_csv(result_uri)
            if filtered_data:
                upload_to_s3(filtered_data, S3_FILE_NAME, aws_credentials)
                download_and_process_csv(result_uri)
                # update_snowflake_table(filtered_data, snowflake_credentials, STAGE_NAME)
    error_count = email_wrapper(FILE_PATH)
    update_snowflake_table_from_s3(snowflake_credentials)
    if error_count == 0:
        message = f':impact: Impact PERFORMANCE_DATA tables loaded :white_check_mark: COMPLETED :white_check_mark: at {dt.now().strftime("%Y%m%d_%H%M")}'
        slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, message)
    else:
        error_message = f':impact: Impact PERFORMANCE_DATA table load :x: FAILED :x: at {dt.now().strftime("%Y%m%d_%H%M")} with {error_count} errors, check the logs for more details'
        slack_notification(SLACK_CHANNEL, SLACK_API_TOKEN, error_message)


if __name__ == "__main__":
    main()

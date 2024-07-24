import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import logging
import datetime
from datetime import date
import snowflake.connector
from dotenv import load_dotenv
import requests
import os
import logging
import datetime
from datetime import date
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import json
import snowflake.connector

# Load environment variables
file_path = os.path.abspath(os.path.join(__file__, "../.."))
env_path = f"{file_path}/src/.env"
load_dotenv(dotenv_path=env_path)
# Retrieve Snowflake credentials from Heroku environment variables
logging_header = os.environ.get("LOGGING_HEADER")
log_name = os.environ.get("LOGGING_FILE")


def setup_logging(log_name):
    path = os.path.abspath(os.path.join(__file__, "../..")) + "/logs"

    if not os.path.exists(path):
        os.makedirs(path)

    logging.basicConfig(
        filename=f"{path}/{log_name}.log",
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    return logging.getLogger(__name__)


logger = setup_logging(log_name)

# Email wrapper for error handling
def email_wrapper(file_path):
    directory = file_path + "/logs"

    search_phrases = ["ERROR"]
    count = 0

    for filename in os.listdir(directory):

        if filename.endswith(".log"):

            path = os.path.join(directory, filename)

            with open(path, "r") as fp:
                for line in fp:
                    for phrase in search_phrases:
                        if phrase in line:
                            count = +1

    return count


# Send confirmation email on SUCCESS
def send_email(file_path, sender_email, recipient_emails, subject, app_password):
    COMMASPACE = ", "
    directory = f"{file_path}/logs"

    sender = f"{sender_email}"

    # Create the enclosing (outer) message
    outer = MIMEMultipart()

    outer["Subject"] = f"{subject} Automation - SUCCESS -  {date.today()}"

    outer["To"] = COMMASPACE.join(recipient_emails)
    outer["From"] = sender_email

    logger.info("Sending E-mail notification...")
    logger.info(
        f"{subject} data processing is completed at {str(datetime.datetime.now())}"
    )

    email_body = ""

    for filename in os.listdir(directory):

        if filename.endswith(".log"):

            path = os.path.join(directory, filename)

            with open(path, "r") as fp:
                data = fp.read()

            content = data.splitlines()

            for element in content:
                log_parts = element.split(" ", 3)
                email_body += f"<tr><td>{log_parts[0]}</td><td>{log_parts[1]}</td><td>{log_parts[2]}</td><td>{log_parts[3]}</td></tr>"

    html = f"""<html>
                <body>
                        {email_body}
                </body>
              </html>"""

    msg = MIMEText(html, "html")
    outer.attach(msg)

    message = outer.as_string()

    # Connect to the SMTP server and send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, app_password)  # Use your App Password here if needed
        server.sendmail(sender_email, recipient_emails, message)


# Send confirmation email on FAILURE
def send_failure_email(
    file_path, sender_email, recipient_emails, subject, app_password
):
    COMMASPACE = ", "
    directory = f"{file_path}/logs"

    sender_email = f"{sender_email}"

    # Create the enclosing (outer) message
    outer = MIMEMultipart()

    outer["Subject"] = f"{subject} Automation - FAILURE -  {date.today()}"

    outer["To"] = COMMASPACE.join(recipient_emails)
    outer["From"] = sender_email

    logger.info("Sending E-mail notification...")
    logger.info(
        f"{subject} data processing is completed with error(s) at {str(datetime.datetime.now())}"
    )

    email_body = ""

    for filename in os.listdir(directory):

        if filename.endswith(".log"):

            path = os.path.join(directory, filename)

            with open(path, "r") as fp:
                data = fp.read()

            content = data.splitlines()

            for element in content:
                log_parts = element.split(" ", 3)
                email_body += f"<tr><td>{log_parts[0]}</td><td>{log_parts[1]}</td><td>{log_parts[2]}</td><td>{log_parts[3]}</td></tr>"

        html = f"""<html>
                <body>
                    <table border="1">
                        <tr>
                            <th>Date</th>
                            <th>Timestamp</th>
                            <th>Log Level</th>
                            <th>Log Message</th>
                        </tr>
                        {email_body}
                    </table>
                </body>
              </html>"""

    msg = MIMEText(html, "html")
    outer.attach(msg)

    message = outer.as_string()

    # Connect to the SMTP server and send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, app_password)  # Use your App Password here if needed
        server.sendmail(sender_email, recipient_emails, message)


def delete_files_in_directory(directory):
    try:
        file_list = [
            f
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
        ]

        for file_name in file_list:
            file_path = os.path.join(directory, file_name)
            os.remove(file_path)

        logger.info(f"All files in the '{directory}' directory deleted successfully.")
    except Exception as e:
        logger.error(f"Error while deleting files: {str(e)}")


def connect_to_snowflake(account, user, password, warehouse, role):
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            role=role,
            account=account,
            warehouse=warehouse,
        )
        logger.info("Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")


def slack_notification(channel, slack_api_token, message):
    slack_api_url = "https://slack.com/api/chat.postMessage"

    payload = {"channel": channel, "text": message}

    headers = {"Authorization": f"Bearer {slack_api_token}"}

    response = requests.post(slack_api_url, json=payload, headers=headers)

    if response.status_code == 200:
        logger.info("Slack notification sent successfully.")
    else:
        logger.error(f"Error sending Slack notification: {response.text}")


import requests


def slack_notification_2(channel, slack_api_token, message, is_html=False):
    slack_api_url = "https://slack.com/api/chat.postMessage"

    payload = {
        "channel": channel,
        "text": {
            "type": "mrkdwn",  # Use plain_text if message is not HTML
            "text": message,
            # 'mrkdwn': is_html  # Use mrkdwn formatting if message is HTML
        },
    }

    headers = {
        "Authorization": f"Bearer {slack_api_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(slack_api_url, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        logger.info("Slack notification sent successfully.")
    else:
        error_message = f"Error sending Slack notification: {response.text}"
        logger.error(error_message)
        print(error_message)  # Print the error message for debugging


def tables_metadata(connection, database, schema, target_table):
    try:
        # Create a cursor object
        cursor = connection.cursor()
        cursor.execute("USE ROLE ACCOUNTADMIN;")
        # Execute the query
        cursor.execute(
            f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE
                        FROM {database}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{schema}'
                        AND TABLE_NAME = '{target_table}';
                    """
        )

        # Fetch all rows from the executed query
        rows = cursor.fetchall()

        # # Close the cursor and connection objects
        # cursor.close()
        connection.close()

        # Format the rows as an HTML table
        table_html = "<table class='my-table' style='border: 1px solid black;'>"
        table_html += f"<caption>{schema}.{target_table}</caption>"
        table_html += "<tr style='background-color: gray; color: white;'>"
        table_html += "<th style='width: 300px;'>Column Name</th>"
        table_html += "<th>Data Type</th>"
        table_html += "</tr>"
        for row in rows:
            table_html += "<tr>"
            table_html += f"<td>{row[0]}</td>"
            table_html += f"<td>{row[1]}</td>"
            table_html += "</tr>"
            table_html += "<tr style='border-top: 1px dashed black;'></tr>"
        table_html += "</table>"

        # Create the directory if it doesn't exist
        directory = os.path.join(os.path.dirname(__file__), "meta_data")
        os.makedirs(directory, exist_ok=True)

        # Save the HTML table to a file in the "meta_data" directory
        html_file_path = os.path.join(directory, f"tables_metadata_.html")
        with open(html_file_path, "w") as file:
            file.write(table_html)

        # Create a figure and axis
        fig, ax = plt.subplots(figsize=(5, 10))
        plt.title(f"{schema}.{target_table}", fontsize=14)  # Add table name as title
        image_file_path = os.path.join(directory, "tables_metadata.png")

        # Assuming you have 'n' rows in your table
        n = len(rows)

        # Create a table within the plot
        table = ax.table(
            cellText=rows,
            colLabels=["Column Name", "Data Type"],
            cellLoc="center",
            loc="center",
            rowLoc="center",  # Add this line for row spacing
            bbox=[0, 0, 1, 1],
        )  # Add this line to set the entire)

        # Set the background color for the first row (header)
        table.auto_set_font_size(False)
        table.set_fontsize(10)

        # Iterate through rows and columns to set background colors
        for i in range(n):
            for j in range(2):  # Assuming 2 columns in the table
                if i == 0:  # Set background color for the header row
                    table[(i, j)].set_facecolor("#D3D3D3")  # Grey color
                else:
                    table[(i, j)].set_facecolor("#FFFFFF")  # White color for other rows

        # Remove outer box around the table
        ax.axis("off")
        # Save the figure to a PNG file with tight layout
        plt.savefig(image_file_path, bbox_inches="tight", pad_inches=0.1)

    except Exception as e:
        logger.error(f"Error getting tables metadata: {e}")

        # Return the rows
        # return rows
        # with open(html_file_path, 'w') as file:
        #     file.write(table_html)

        # # Save the HTML table as an image
        # image_file_path = os.path.join(directory, 'tables_metadata.png')
        # # imgkit.from_file(html_file_path, image_file_path, options={'format': 'png'})
        # plt.figure(figsize=(10, 10))
        # plt.savefig('tables_metadata.png')  # Save the figure to a PNG file

        # Return the rows
        # return rows
    except Exception as e:
        logger.error(f"Error getting tables metadata: {e}")

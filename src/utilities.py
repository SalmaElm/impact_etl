import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import logging
import datetime
from datetime import date

# Error Logging
path = os.path.abspath(os.path.join(__file__, "../..")) + '/logs'

with open(os.path.join(path, 'impact_log.log'), 'w'):
    pass

logging.basicConfig(filename=f'{path}/impact_log.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')

logger = logging.getLogger(__name__)

file_ts = datetime.datetime.now().strftime("%y%m%d%H%M%S")

logger.info(f'Impact report data load begins at {str(datetime.datetime.now())}...')

# Email wrapper for error handling
def email_wrapper(file_path, recipient_emails):
    directory = file_path + '/logs'

    search_phrases = ["ERROR"]
    count = 0

    for filename in os.listdir(directory):

        if filename.endswith('.log'):

            path = os.path.join(directory, filename)

            with open(path, "r") as fp:
                for line in fp:
                    for phrase in search_phrases:
                        if phrase in line:
                            count = +1

    return count

# Send confirmation email on SUCCESS
def send_email(file_path, sender_email, recipient_emails, subject, app_password):
    COMMASPACE = ', '
    directory = f'{file_path}/logs'

    sender = f'{sender_email}'

    # Create the enclosing (outer) message
    outer = MIMEMultipart()

    outer['Subject'] = f'{subject} Automation - SUCCESS -  {date.today()}'

    outer['To'] = COMMASPACE.join(recipient_emails)
    outer['From'] = sender_email

    logger.info('Sending E-mail notification...')
    logger.info(f'{subject} data processing is completed at {str(datetime.datetime.now())}')

    email_body = ""

    for filename in os.listdir(directory):

        if filename.endswith('.log'):

            path = os.path.join(directory, filename)

            with open(path, "r") as fp:
                data = fp.read()

            content = data.splitlines()

            for element in content:
                log_parts = element.split(" ", 3)
                email_body += f'<tr><td>{log_parts[0]}</td><td>{log_parts[1]}</td><td>{log_parts[2]}</td><td>{log_parts[3]}</td></tr>'

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

    msg = MIMEText(html, 'html')
    outer.attach(msg)

    message = outer.as_string()

    # Connect to the SMTP server and send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, app_password)  # Use your App Password here if needed
        server.sendmail(sender_email, recipient_emails, message)

# Send confirmation email on FAILURE
def send_failure_email(file_path, sender_email, recipient_emails, subject, app_password):
    COMMASPACE = ', '
    directory = f'{file_path}/logs'

    sender_email = f'{sender_email}'

    # Create the enclosing (outer) message
    outer = MIMEMultipart()

    outer['Subject'] = f'{subject} Automation - FAILURE -  {date.today()}'

    outer['To'] = COMMASPACE.join(recipient_emails)
    outer['From'] = sender_email

    logger.info('Sending E-mail notification...')
    logger.info(f'{subject} data processing is completed with error(s) at {str(datetime.datetime.now())}')

    email_body = ""

    for filename in os.listdir(directory):

        if filename.endswith('.log'):

            path = os.path.join(directory, filename)

            with open(path, "r") as fp:
                data = fp.read()

            content = data.splitlines()

            for element in content:
                log_parts = element.split(" ", 3)
                email_body += f'<tr><td>{log_parts[0]}</td><td>{log_parts[1]}</td><td>{log_parts[2]}</td><td>{log_parts[3]}</td></tr>'

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

    msg = MIMEText(html, 'html')
    outer.attach(msg)

    message = outer.as_string()

    # Connect to the SMTP server and send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, app_password)  # Use your App Password here if needed
        server.sendmail(sender_email, recipient_emails, message)

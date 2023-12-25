import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def send_email(subject, body, sender_email, app_password, recipient_email, attachment_path=None):
    # Set up the email content
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient_email
    message["Subject"] = subject

        # Attach the log content as plain text in the email body
    message.attach(MIMEText(body, "plain"))
    
    # Connect to the SMTP server and send the email
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, app_password)  # Use your App Password here if needed
        server.sendmail(sender_email, recipient_email, message.as_string())

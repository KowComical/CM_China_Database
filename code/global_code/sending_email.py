import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import time

SMTP_SERVER = "smtp.gmail.com"
SENDER_EMAIL = "####"
SENDER_PASSWORD = "####"


def main(file_path):
    # List of recipient email addresses
    recipients = ["kowdataanalyze@gmail.com"]

    # Extract the date from the log file name
    date_str = os.path.basename(file_path).split('-')[1:4]
    formatted_date = '.'.join(date_str).replace('.out', '')

    # Format the email subject using the extracted date
    subject = f"CM_China {formatted_date}"

    # Read the content of the log file
    with open(file_path, "r") as f:
        body = f.read()

    # Call the function to send the email with the log file content as the body
    send_email(SENDER_EMAIL, recipients, subject, body)


def send_email(sender, recipients, subject, body, smtp_ports=(25, 465, 587), max_retries=3, retry_delay=10):
    for port in smtp_ports:
        success = False
        for i in range(max_retries):
            try:
                # Create the email message
                message = MIMEMultipart()
                message["From"] = sender
                message["To"] = ", ".join(recipients)
                message["Subject"] = subject
                message.attach(MIMEText(body))

                # Connect to the SMTP server and send the email
                with smtplib.SMTP(SMTP_SERVER, port) as smtp_server:
                    smtp_server.ehlo()
                    if port != 25:
                        smtp_server.starttls()
                    smtp_server.login(sender, SENDER_PASSWORD)
                    smtp_server.sendmail(sender, recipients, message.as_string())

                # If the email is sent successfully, break the loop and set success flag
                success = True
                break
            except Exception as e:
                if i < max_retries - 1:
                    print(f"Error sending email on port {port}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print(f"Error sending email on port {port}. Reached maximum retries. Exception: {e}")

        if success:
            break
    else:
        print("Failed to send email on all specified ports.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        log_file_path = sys.argv[1]
        main(log_file_path)
    else:
        print("Error: Log file path is not provided.")

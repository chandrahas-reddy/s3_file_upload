"""
Author: Chandrahas Dodda

Readme:

Mailer
This file consists of a class which sends status email after file upload.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import Config

class Mailer:
    def send_mail(self, source, target, status):
        print("mail sent!")

import os
import email
import smtplib
import ssl

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Mailer:
    def __init__(self, jobname):
        self.jobname = jobname
        self.user = os.getenv("SENDER_EMAIL")
        self.password = os.getenv("SENDER_PWD")
        self.to_email = os.getenv("RECIPIENT_EMAIL")
        context = ssl.create_default_context()
        self.server = smtplib.SMTP_SSL(
            os.getenv("EMAIL_SERVER"), os.getenv("EMAIL_PORT"), context=context
        )

    def _subject_line(self):
        subject_type = "Sucess" if self.success else "Error"
        return f"{self.jobname} - {subject_type}"

    def _body_text(self):
        if self.success:
            return f"{self.jobname} completed successfully.\n{self.logs}"
        else:
            return f"{self.jobname} encountered an error.\n{self.logs}"

    def _read_logs(self, filename):
        with open(filename) as f:
            return f.read()

    def _attachments(self, msg):
        filename = "app.log"
        if os.path.exists(filename):
            with open(filename, "r") as attachment:
                log = MIMEText(attachment.read())
            log.add_header("Content-Disposition", f"attachment; filename= {filename}")
            msg.attach(log)

    def _message(self):
        msg = MIMEMultipart()
        msg["Subject"] = self._subject_line()
        msg["From"] = self.user
        msg["To"] = self.to_email
        msg.attach(MIMEText(self._body_text(), "plain"))
        self._attachments(msg)
        return msg.as_string()

    def notify(self, success):
        self.success = success
        self.logs = self._read_logs("app.log")
        with self.server as s:
            s.login(self.user, self.password)
            msg = self._message()
            s.sendmail(self.user, self.to_email, msg)

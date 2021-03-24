from datetime import datetime, timedelta
from fnmatch import fnmatch
import logging
import os
import sys
import traceback

import pandas as pd
from sqlsorcery import MSSQL

from browser import Browser
from config import data_reports
from ftp import FTP
from mailer import Mailer


logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="app.log", mode="w+"),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p %Z",
)


class Connector:
    """ETL connector class"""

    def __init__(self):
        self.data_dir = "data"
        self.sql = MSSQL()
        self.ftp = FTP(self.data_dir)

    def sync_all_ftp_data(self):
        for key, value in data_reports.items():
            table_name = key
            directory_name = value
            self._sync_data_from_ftp(table_name, directory_name)

    def _sync_data_from_ftp(self, table_name, directory_name):
        """Download data from the given directory and insert new records into the data warehouse."""
        self.ftp.download_files(directory_name)
        latest_date = self._get_latest_date(table_name)
        one_day = timedelta(days=1)
        start_date = latest_date + one_day  # earliest date needed
        yesterday = datetime.today() - one_day
        if start_date > yesterday:
            logging.info(f"Clever_{table_name} is up to date. No records inserted.")
            return
        else:
            file_names = self._generate_expected_file_names(
                start_date, yesterday, directory_name
            )
            df = self._read_and_concat_files(file_names)
            self.sql.insert_into(f"Clever_{table_name}", df, if_exists="append")
            logging.info(f"Inserted {len(df)} records into Clever_{table_name}.")

    def _get_latest_date(self, table_name):
        """Get the latest date record in this table."""
        date = self.sql.query(
            f"SELECT TOP(1) [date] FROM custom.Clever_{table_name} ORDER BY [date] DESC"
        )
        latest_date = date["date"][0]
        return datetime.strptime(latest_date, "%Y-%m-%d")

    def _generate_expected_file_names(self, start_date, yesterday, directory_name):
        file_names = []
        while start_date <= yesterday:  # loop through yesterday's date
            formatted_date = start_date.strftime("%Y-%m-%d")
            file_names.append(f"{formatted_date}-{directory_name}-students.csv")
            start_date += one_day
        return file_names

    def _read_and_concat_files(self, file_names):
        dfs = []
        for file_name in file_names:
            df = pd.read_csv(f"{self.data_dir}/{file_name}")
            dfs.append(df)
        data = pd.concat(dfs)
        return data

    def sync_student_google_accounts(self):
        """Get student emails from Google Accounts Manager app."""
        browser = Browser(self.data_dir)
        browser.export_student_google_accounts()
        # Transform and load csv data into database table
        df = self._get_data_from_csv_by_name("Student_export")
        df.rename(columns={"ID": "SIS_ID"}, inplace=True)
        sql.insert_into("Clever_StudentGoogleAccounts", df, if_exists="replace")
        logging.info(
            f"Inserted {len(df)} new records into Clever_StudentGoogleAccounts."
        )

    def _get_data_from_csv_by_name(self, string_to_match):
        """Get the downloaded csv BY NAME and store it in a dataframe."""
        for filename in os.listdir(self.data_dir):
            if fnmatch(filename, f"*{string_to_match}*"):
                file_path = f"{self.data_dir}/{filename}"
                break
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} records from downloaded file.")
        return df


def main():
    connector = Connector()
    connector.sync_all_ftp_data()
    connector.sync_student_google_accounts(sql, browser)


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("Clever").notify(error_message=error_message)

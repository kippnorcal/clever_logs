from datetime import datetime, timedelta
from fnmatch import fnmatch
import logging
import os
import sys
import traceback

import pandas as pd
from sqlsorcery import MSSQL

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
        for table_name, directory_name in data_reports.items():
            self.ftp.download_files(directory_name)
            self._load_new_records_into_table(table_name, directory_name)

    def _load_new_records_into_table(self, table_name, report_name):
        """Find and insert new records into the data warehouse."""
        if report_name == "idm-reports":
            # this folder contains student emails file, which has no datestamp in the file name
            self._process_files_without_datestamp(table_name, report_name)
        else:
            self._process_files_with_datestamp(table_name, report_name)

    def _process_files_without_datestamp(self, table_name, report_name):
        # Student Emails file doesn't contain a datestamp in the file name
        # This table should be truncated and replaced.
        df = self._read_file(f"{self.data_dir}/google-student-emails.csv")
        self.sql.insert_into(f"Clever_{table_name}", df, if_exists="replace")
        logging.info(f"Inserted {len(df)} records into Clever_{table_name}.")

    def _process_files_with_datestamp(self, table_name, report_name):
        # Generate names for files with datestamps in the file name and process those files
        # These tables should be appended to, not truncated.
        start_date = self._get_latest_date(table_name) + timedelta(days=1)
        yesterday = datetime.today() - timedelta(days=1)
        if start_date > yesterday:
            logging.info(f"Clever_{table_name} is up to date. No records inserted.")
            return
        else:
            file_names = self._generate_file_names(start_date, yesterday, report_name)
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

    def _generate_file_names(self, start_date, yesterday, report_name):
        file_names = []
        while start_date <= yesterday:  # loop through yesterday's date
            formatted_date = start_date.strftime("%Y-%m-%d")
            file_names.append(f"{formatted_date}-{report_name}-students.csv")
            start_date += timedelta(days=1)
        return file_names

    def _read_and_concat_files(self, file_names):
        dfs = []
        for file_name in file_names:
            try:
                df = pd.read_csv(f"{self.data_dir}/{file_name}")
                logging.info(f"Read {len(df)} records from '{file_name}'.")
                dfs.append(df)
            except FileNotFoundError as e:
                logging.info(f"{file_name} Does not exist: \n{e}")
        data = pd.concat(dfs)
        return data

    def _read_file(self, file_name):
        df = pd.read_csv(file_name)
        logging.info(f"Read {len(df)} records from '{file_name}'.")
        return df


def main():
    connector = Connector()
    connector.sync_all_ftp_data()


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("Clever").notify(error_message=error_message)

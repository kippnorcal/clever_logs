from datetime import datetime, timedelta
import logging
import sys
import traceback

import pandas as pd
from sqlsorcery import MSSQL

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

"""
Commented out "Participation" and "Resource_Usage" as we currently do not need this data.
These reports use the Connector._process_files_with_datestamp method. Keeping this code just in case 
we ever need to use it again.
"""

DATA_REPORTS = {
    # "Participation": "daily-participation",
    # "Resource_Usage": "resource-usage",
    "StudentGoogleAccounts": "idm-reports"
}


def _process_files_without_datestamp(table_name, sql):
    # Student Emails file doesn't contain a datestamp in the file name
    # This table should be truncated and replaced.
    df = _read_file(f"data/google-student-emails.csv")
    sql.insert_into(f"Clever_{table_name}", df, if_exists="replace")
    logging.info(f"Inserted {len(df)} records into Clever_{table_name}.")

def _process_files_with_datestamp(table_name, report_name, sql):
    # Generate names for files with datestamps in the file name and process those files
    # These tables should be appended to, not truncated.
    start_date = _get_latest_date(table_name) + timedelta(days=1)
    yesterday = datetime.today() - timedelta(days=1)
    if start_date > yesterday:
        logging.info(f"Clever_{table_name} is up to date. No records inserted.")
        return
    else:
        file_names = _generate_file_names(start_date, yesterday, report_name)
        df = _read_and_concat_files(file_names)
        if df:
            sql.insert_into(f"Clever_{table_name}", df, if_exists="append")
            logging.info(f"Inserted {len(df)} records into Clever_{table_name}.")
        else:
            logging.info(f"No records to insert into Clever_{table_name}.")

def _get_latest_date(table_name, sql):
    """Get the latest date record in this table."""
    date = sql.query(
        f"SELECT TOP(1) [date] FROM custom.Clever_{table_name} ORDER BY [date] DESC"
    )
    latest_date = date["date"][0]
    return datetime.strptime(latest_date, "%Y-%m-%d")


def _generate_file_names(start_date, yesterday, report_name):
    file_names = []
    while start_date <= yesterday:  # loop through yesterday's date
        formatted_date = start_date.strftime("%Y-%m-%d")
        file_names.append(f"{formatted_date}-{report_name}-students.csv")
        start_date += timedelta(days=1)
    return file_names


def _read_and_concat_files(file_names):
    dfs = []
    for file_name in file_names:
        try:
            file_name = f"data/{file_name}"
            df = _read_file(file_name)
            dfs.append(df)
        except FileNotFoundError as e:
            logging.info(f"{file_name} Does not exist: \n{e}")
    if dfs:
        return pd.concat(dfs)
    else:
        return None


def _read_file(file_name):
    df = pd.read_csv(file_name)
    logging.info(f"Read {len(df)} records from '{file_name}'.")
    return df


def main():
    ftp = FTP("data")
    for table_name, directory_name in DATA_REPORTS.items():
        ftp.download_files(directory_name)
        if directory_name == "idm-reports":
            _process_files_without_datestamp(table_name)
        else:
            _process_files_with_datestamp(table_name, directory_name)



    connector = Connector()
    connector.sync_all_ftp_data()


if __name__ == "__main__":
    try:
        main()
        success = True
    except Exception as e:
        logging.exception(e)
        logging.info(traceback.format_exc())
        success = False
    Mailer("Clever").notify(success)

from datetime import datetime, timedelta
import logging
import os
import sys
import traceback
from typing import List, Union

from gbq_connector import BigQueryClient, CloudStorageClient, DbtClient
from job_notifications import create_notifications
import pandas as pd
import pysftp


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
    # "participation": "daily-participation",
    # "resource_usage": "resource-usage",
    "google-student-emails": "idm-reports"
}
LOCAL_DIR = "data"
BUCKET = os.getenv("BUCKET")

notifications = create_notifications("Clever", "mailgun", logs="app.log")

def _upload_file(table_name: str, file_name, data: pd.DataFrame, cloud_client: CloudStorageClient) -> None:
    blob = f"clever/{table_name}/{file_name}"
    cloud_client.load_dataframe_to_cloud_as_csv(BUCKET, blob, data)
    logging.info(f"Inserted {len(data)} records into {table_name}.")


def _process_files_with_datestamp(table_name: str, report_name: str, start_date: datetime, cloud_client: CloudStorageClient) -> None:
    # Generate names for files with datestamps in the file name and process those files
    # These tables should be appended to, not truncated.
    yesterday = datetime.today() - timedelta(days=1)
    if start_date > yesterday:
        logging.info(f"base_clever_{table_name} is up to date. No records inserted.")
    else:
        file_names = _generate_file_names(start_date, yesterday, report_name)
        if df:
            for file_name in file_names:
                try:
                    file_path = os.path.join(LOCAL_DIR, file_name)
                    df = _read_file(file_path)
                    _upload_file(table_name, file_name, df, cloud_client)
                except FileNotFoundError as e:
                    logging.info(f"Error {file_path}: {e}")        
        else:
            logging.info(f"No records to insert into Clever_{table_name}.")


def _get_latest_date(table_name: str, bq_conn: BigQueryClient) -> datetime:
    """Get the latest date record in this table."""
    result = bq_conn.query(f"SELECT MAX(date) FROM `base_clever_{table_name}`")
    times_stamp = result.iloc[0, 0].strftime('%Y-%m-%d')
    return datetime.strptime(times_stamp, "%Y-%m-%d")


def _generate_file_names(start_date: datetime, yesterday: datetime, report_name: str) -> List[str]:
    file_names = []
    while start_date <= yesterday:  # loop through yesterday's date
        formatted_date = start_date.strftime("%Y-%m-%d")
        file_names.append(f"{formatted_date}-{report_name}-students.csv")
        start_date += timedelta(days=1)
    return file_names


def _read_file(file_name: str) -> pd.DataFrame:
    df = pd.read_csv(file_name, dtype='string')
    logging.info(f"Read {len(df)} records from '{file_name}'.")
    return df


def main():
    cloud_client = CloudStorageClient()
    bq_conn = BigQueryClient()
    dbt_client = DbtClient()

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    ftp = pysftp.Connection(
            host=os.getenv("FTP_HOST"),
            username=os.getenv("FTP_USER"),
            password=os.getenv("FTP_PW"),
            cnopts=cnopts
        )

    for table_name, directory_name in DATA_REPORTS.items():
        ftp.get_d(directory_name, LOCAL_DIR, preserve_mtime=True)
        if directory_name == "idm-reports":
            file_name = f"{table_name}.csv"
            file_path = os.path.join(LOCAL_DIR, file_name)
            df = _read_file(file_path)
            _upload_file(table_name, file_name, df, cloud_client)
        else:
            start_date = _get_latest_date(table_name, bq_conn) + timedelta(days=1)
            _process_files_with_datestamp(table_name, directory_name, start_date, cloud_client)

    logging.info("Running dbt job")
    dbt_client.run_job()


if __name__ == "__main__":
    try:
        main()
        notifications.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        notifications.notify(error_message=stack_trace)

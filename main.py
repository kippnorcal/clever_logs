from fnmatch import fnmatch
import logging
import os
import sys
import traceback

import pandas as pd
from sqlsorcery import MSSQL

from browser import Browser
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

DATA_DIR = "data"


def get_data_from_csv_by_name(string_to_match):
    """Get the downloaded csv BY NAME and store it in a dataframe."""
    for filename in os.listdir(DATA_DIR):
        if fnmatch(filename, f"*{string_to_match}*"):
            file_path = f"{DATA_DIR}/{filename}"
            break
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {len(df)} records from downloaded file.")
    return df


def load_newest_data(sql, df, table_name):
    """Insert newest records into the database.

    Params:
        sql (MSSQL): SqlSorcery object
        df (DataFrame): data to diff and insert into database
        table_name (str): name of the table we're inserting into
    """
    date = sql.query(
        f"SELECT TOP(1) [date] FROM custom.{table_name} ORDER BY [date] DESC"
    )
    latest_date = date["date"][0]
    df = df[df["date"] > latest_date]
    sql.insert_into(table_name, df)
    logging.info(f"Inserted {len(df)} new records into {table_name}.")


def get_student_google_accounts(sql, browser):
    """Get student emails from Google Accounts Manager app."""
    browser.export_student_google_accounts()
    # Transform and load csv data into database table
    df = get_data_from_csv_by_name("Student_export")
    df.rename(columns={"ID": "SIS_ID"}, inplace=True)
    sql.insert_into("Clever_StudentGoogleAccounts", df, if_exists="replace")
    logging.info(f"Inserted {len(df)} new records into Clever_StudentGoogleAccounts.")


def main():
    browser = Browser(DATA_DIR)
    sql = MSSQL()
    # TODO get student participation
    # TODO get student resource usage
    get_student_google_accounts(sql, browser)


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("Clever").notify(error_message=error_message)

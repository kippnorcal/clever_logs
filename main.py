import datetime
from datetime import datetime as dt
from fnmatch import fnmatch
import glob
import logging
import json
import os
import sys
import time
import traceback

import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from sqlsorcery import MSSQL
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from tenacity import retry, stop_after_attempt, wait_exponential, Retrying

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

DATA_DIR = os.path.join(os.getcwd(), "data")


def create_driver():
    """Create Chrome webdriver with the specified preferences for automatic downloading."""
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")  # chrome crashes without this arg
    options.add_argument("--headless")
    options.add_argument("--browser.download.folderList=2")
    options.add_argument("--browser.helperApps.neverAsk.saveToDisk=text/csv")
    prefs = {"download.default_directory": DATA_DIR}
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)
    return driver


def login(driver):
    """Sign into Clever"""
    driver.get(
        "https://clever.com/oauth/authorize?channel=clever&client_id=4c63c1cf623dce82caac&confirmed=true&redirect_uri=https%3A%2F%2Fclever.com%2Fin%2Fauth_callback&response_type=code&state=8460e9c3c0026c4ef2532f43a5ab5b9bcfbadbbe516f4589544cb726fd579ecf&user_type=district_admin"
    )
    time.sleep(5)
    user_field = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//input[@name="username"]'))
    )
    user_field.send_keys(os.getenv("CLEVER_USER"))
    password_field = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//input[@name="password"]'))
    )
    password_field.send_keys(os.getenv("CLEVER_PW"))
    submit_button = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//button[@aria-label="Log in"]'))
    )
    submit_button.click()
    logging.info("Successfully logged in.")


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=4, max=10))
def check_for_file_download(file_string):
    """Check if a file is downloaded based on a partial string match"""
    files = glob.glob(f"{DATA_DIR}/{file_string}")
    if len(files) == 0:
        raise Exception(f"'{file_string}' file not found.")


def export_login_logs(driver):
    """Download Login Logs csv"""
    driver.get("https://schools.clever.com/instant-login/logs")
    time.sleep(5)
    export_button = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//a[@aria-label="Export as .csv"]'))
    )
    export_button.click()
    check_for_file_download("*logins.csv")
    time.sleep(5)
    logging.info("Successfully downloaded login logs.")


def parse_email(df):
    """Extract email from attributes column json."""
    df["attributes"] = df["attributes"].str.replace("\\", "")
    attributes = pd.json_normalize(df["attributes"].apply(json.loads))
    df = df.join(attributes.email)
    df.replace(np.nan, "", inplace=True)
    return df


def get_data_from_csv(sql):
    """Get the downloaded csv and store it in a dataframe."""
    # the filename sometimes gets cut off due to a Clever bug,
    # so we're not searching for it by the .csv extension
    for filename in os.listdir(DATA_DIR):
        if fnmatch(filename, f"*logins*"):
            file_path = f"{DATA_DIR}/{filename}"
            break
    df = pd.read_csv(file_path)
    df = parse_email(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.astype({"timestamp": "datetime64[ns]"})
    logging.info(f"Loaded {len(df)} records from downloaded file.")
    return df


def get_data_from_csv_by_name(string_to_match):
    """Get the downloaded csv BY NAME and store it in a dataframe."""
    for filename in os.listdir(DATA_DIR):
        if fnmatch(filename, f"*{string_to_match}*"):
            file_path = f"{DATA_DIR}/{filename}"
            break
    df = pd.read_csv(file_path)
    logging.info(f"Loaded {len(df)} records from downloaded file.")
    return df


def load_newest_data(sql, df):
    """Insert newest records into the database."""
    time = sql.query(
        "SELECT TOP(1) timestamp FROM custom.Clever_LoginLogs ORDER BY timestamp DESC"
    )
    latest_timestamp = time["timestamp"][0]
    df = df[df["timestamp"] > latest_timestamp]
    sql.insert_into("Clever_LoginLogs", df)
    logging.info(f"Inserted {len(df)} new records into Clever_LoginLogs.")


def close(driver):
    driver.close()


def get_logs(sql, driver):
    """Download logs data from Clever"""
    export_login_logs(driver)
    # Transform and load csv data into database table
    df = get_data_from_csv(sql)
    load_newest_data(sql, df)


def export_student_users(driver):
    """Download csv of student users"""
    driver.get("https://schools.clever.com/browser")
    time.sleep(10)
    # add student filter (doesn't work if you navigate directly there)
    driver.get(
        "https://schools.clever.com/browser#{%22sidebar%22:%22students%22,%22filters%22:[]}"
    )
    time.sleep(15)
    download_button = driver.find_element_by_id("download-all")
    download_button.click()
    check_for_file_download("students.csv")
    time.sleep(5)
    logging.info("Successfully downloaded student users.")


def get_student_user_table(sql, driver):
    """Get student users from Clever Data Browser."""
    export_student_users(driver)
    # Transform and load csv data into database table
    df = get_data_from_csv_by_name("students")
    sql.insert_into("Clever_StudentUsers", df, if_exists="replace")
    logging.info(f"Inserted {len(df)} new records into Clever_StudentUsers.")


def export_student_google_accounts(driver):
    """Download student google accounts csv"""
    driver.get(
        "https://schools.clever.com/school/applications/50ca15a93bc2733956000007/settings"
    )
    time.sleep(5)
    iframe = driver.find_element_by_name("canvas_iframe")
    driver.switch_to.frame(iframe)
    time.sleep(5)
    export_button = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.LINK_TEXT, "Student Export"))
    )
    export_button.click()
    check_for_file_download("Student_export*.csv")
    time.sleep(5)
    logging.info("Successfully downloaded student google accounts.")


def get_student_google_accounts(sql, driver):
    """Get student emails from Google Accounts Manager app."""
    export_student_google_accounts(driver)
    # Transform and load csv data into database table
    df = get_data_from_csv_by_name("Student_export")
    df.rename(columns={"ID": "SIS_ID"}, inplace=True)
    sql.insert_into("Clever_StudentGoogleAccounts", df, if_exists="replace")
    logging.info(f"Inserted {len(df)} new records into Clever_StudentGoogleAccounts.")


def main():
    driver = create_driver()
    driver.implicitly_wait(5)
    login(driver)
    sql = MSSQL()
    get_logs(sql, driver)
    get_student_user_table(sql, driver)
    get_student_google_accounts(sql, driver)


if __name__ == "__main__":
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    Mailer("Clever Login Logs").notify(error_message=error_message)

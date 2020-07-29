import datetime
from datetime import datetime as dt
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


def create_driver():
    """Create firefox webdriver with the specified preference for automatic downloading."""
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.manager.showWhenStarting", False)
    profile.set_preference("browser.download.dir", os.getcwd())
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv")
    profile.set_preference("pdfjs.disabled", True)
    return webdriver.Firefox(firefox_profile=profile)


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


def export_login_logs(driver):
    """Download Log Logs csv"""
    driver.get("https://schools.clever.com/instant-login/logs")
    time.sleep(5)
    export_button = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//a[@aria-label="Export as .csv"]'))
    )
    export_button.click()
    logging.info("Successfully downloaded login logs.")


def parse_email(df):
    attributes = pd.json_normalize(df["attributes"].apply(json.loads))
    df = df.join(attributes.email)
    df.replace(np.nan, "", inplace=True)
    return df


def get_data_from_csv(sql):
    result = glob.glob("*.csv")
    file_path = result[0]
    df = pd.read_csv(file_path)
    df = parse_email(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.astype({"timestamp": "datetime64[ns]"})
    logging.info(f"Loaded {len(df)} records from downloaded file.")
    return df


def load_newest_data(sql, df):
    time = sql.query(
        "SELECT TOP(1) timestamp FROM custom.Clever_LoginLogs ORDER BY timestamp DESC"
    )
    latest_timestamp = time["timestamp"][0]
    df = df[df["timestamp"] > latest_timestamp]
    sql.insert_into("Clever_LoginLogs", df)
    logging.info(f"Inserted {len(df)} new records into Clever_LoginLogs")


def close(driver):
    driver.close()


def main():
    try:
        sql = MSSQL()
        driver = create_driver()
        driver.implicitly_wait(5)
        login(driver)
        export_login_logs(driver)
        df = get_data_from_csv(sql)
        load_newest_data(sql, df)
        Mailer("Clever Login Logs").notify()
    except Exception as e:
        logging.exception(e)
        Mailer("Clever Login Logs").notify(error=True)
    finally:
        close(driver)


if __name__ == "__main__":
    main()

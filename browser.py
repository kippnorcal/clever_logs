import glob
import logging
import os
import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from tenacity import retry, stop_after_attempt, wait_exponential, Retrying


class Browser:
    """
    Selenium Chrome driver instance.
    """

    def __init__(self, download_dir):
        """Create Chrome webdriver with the specified preferences for automatic downloading."""
        self.download_dir = download_dir
        options = webdriver.ChromeOptions()
        options.add_argument("--no-sandbox")  # chrome crashes without this arg
        options.add_argument("--headless")
        options.add_argument("--browser.download.folderList=2")
        options.add_argument("--browser.helperApps.neverAsk.saveToDisk=text/csv")
        prefs = {"download.default_directory": self.download_dir}
        options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(10)
        self._login()

    def _login(self):
        """Sign into Clever"""
        self.driver.get(
            "https://clever.com/oauth/authorize?channel=clever&client_id=4c63c1cf623dce82caac&confirmed=true&redirect_uri=https%3A%2F%2Fclever.com%2Fin%2Fauth_callback&response_type=code&state=8460e9c3c0026c4ef2532f43a5ab5b9bcfbadbbe516f4589544cb726fd579ecf&user_type=district_admin"
        )
        time.sleep(5)
        user_field = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//input[@name="username"]'))
        )
        user_field.send_keys(os.getenv("CLEVER_USER"))
        password_field = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//input[@name="password"]'))
        )
        password_field.send_keys(os.getenv("CLEVER_PW"))
        submit_button = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '//button[@aria-label="Log in"]'))
        )
        submit_button.click()
        logging.info("Successfully logged in.")

    @retry(
        stop=stop_after_attempt(10), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _check_for_file_download(self, file_string):
        """Check if a file is downloaded based on a partial string match"""
        files = glob.glob(f"{self.download_dir}/{file_string}")
        if len(files) == 0:
            raise Exception(f"'{file_string}' file not found.")

    def export_student_google_accounts(self):
        """Download student google accounts csv"""
        self.driver.get(
            "https://schools.clever.com/school/applications/50ca15a93bc2733956000007/settings"
        )
        time.sleep(5)
        iframe = self.driver.find_element_by_name("canvas_iframe")
        self.driver.switch_to.frame(iframe)
        time.sleep(5)
        export_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.LINK_TEXT, "Student Export"))
        )
        export_button.click()
        self._check_for_file_download("Student_export*.csv")
        time.sleep(5)
        logging.info("Successfully downloaded student google accounts.")

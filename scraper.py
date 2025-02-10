import os
import time
import logging
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyvirtualdisplay import Display

# PostgreSQL connection details
DB_NAME = "cdc"
DB_USER = "postgres"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

# Set up virtual display for headless mode
display = Display(visible=0, size=(1920, 1080))
display.start()

# Logging configuration
logging.basicConfig(level=logging.INFO)

# Set up Chrome options
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--disable-gpu')
options.add_argument('--window-size=1920x1080')

# Set up download directory
#download_directory = "web_scrapping/downloads"
download_directory = os.path.abspath("/home/habtech/web_scrapping/web_scrapping/downloads")
os.makedirs(download_directory, exist_ok=True)
#prefs = {"download.default_directory": download_directory}
#options.add_experimental_option("prefs", prefs)
prefs = {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

# Initialize WebDriver
service = Service('myenv/bin/chromedriver')
driver = webdriver.Chrome(service=service, options=options)

try:
    logging.info("Navigating to webpage...")
    driver.get('https://data.cdc.gov/Flu-Vaccinations/Vaccines-gov-Flu-vaccinating-provider-locations/bugr-bbfr/about_data')

    # Wait for the page to load
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    logging.info("Page loaded.")

    # Click Export button
    export_button_xpath = '//forge-button[@data-testid="export-data-button"]'
    WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, export_button_xpath))).click()
    logging.info("Clicked Export button.")

    # Wait for modal
    modal_xpath = '//div[@class="export-dataset-dialog"]'
    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, modal_xpath)))

    # Click Download button
    download_button_xpath = '//button[@data-testid="export-download-button"]'
    WebDriverWait(driver, 60).until(EC.element_to_be_clickable((By.XPATH, download_button_xpath))).click()
    logging.info("Clicked Download button.")
    time.sleep(10)  # Adjust sleep time as needed
    print("Checking downloaded files in:", download_directory)
    print("Files:", os.listdir(download_directory))
    # Wait for file to download
    time.sleep(30)

    # Check downloaded file
    downloaded_files = [f for f in os.listdir(download_directory) if f.endswith('.csv')]
    if downloaded_files:
        downloaded_filename = downloaded_files[0]
        downloaded_file_path = os.path.join(download_directory, downloaded_filename)
        logging.info(f'File downloaded successfully: {downloaded_file_path}')

        # Save file info to PostgreSQL
        #conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        #cursor = conn.cursor()
        #cursor.execute("INSERT INTO downloads (filename, filepath) VALUES (%s, %s)", (downloaded_filename, downloaded_file_path))
        #conn.commit()
        #cursor.close()
        #conn.close()
        logging.info("File saved to database.")

    else:
        logging.error("Downloaded file not found.")
        raise FileNotFoundError("No CSV file found.")

except Exception as e:
    logging.error(f"An error occurred: {e}")

finally:
    driver.quit()
    display.stop()

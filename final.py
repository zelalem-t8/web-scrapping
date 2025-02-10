import os
import time
import logging
import psycopg2
import pandas as pd
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyvirtualdisplay import Display

# PostgreSQL connection details
DB_NAME = "cdc"
DB_USER = "postgres"
DB_PASSWORD = "U@habtech22"
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
download_directory = "web_scrapping/downloads"
os.makedirs(download_directory, exist_ok=True)
prefs = {
    "download.default_directory": download_directory,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
options.add_experimental_option("prefs", prefs)

# Initialize the WebDriver
try:
    service = Service('myenv/bin/chromedriver')
    driver = webdriver.Chrome(service=service, options=options)
    print("WebDriver initialized successfully.")
except Exception as e:
    logging.error(f"Failed to initialize WebDriver: {e}")
    raise

try:
    print("Navigating to webpage...")
    driver.get('https://data.cdc.gov/Flu-Vaccinations/Vaccines-gov-Flu-vaccinating-provider-locations/bugr-bbfr/about_data')

    # Wait for the page to load
    WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.TAG_NAME, 'body'))
    )
    print("Page loaded successfully.")

    # Wait for the Export button to appear and click it
    export_button_xpath = '//forge-button[@data-testid="export-data-button"]'
    WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, export_button_xpath))
    ).click()
    print("Clicked on the Export button.")

    # Wait for the modal to appear
    modal_xpath = '//div[@class="export-dataset-dialog"]'
    WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.XPATH, modal_xpath))
    )
    print("Export modal opened successfully.")

    # Wait for the Download button to appear and click it
    download_button_xpath = '//button[@data-testid="export-download-button"]'
    WebDriverWait(driver, 60).until(
        EC.element_to_be_clickable((By.XPATH, download_button_xpath))
    ).click()
    print("Clicked on the Download button.")

    # Wait for the file to download
    time.sleep(30)  # Adjust this as necessary to ensure the file has time to download

    # Check for downloaded file
    downloaded_files = [f for f in os.listdir(download_directory) if f.endswith('.csv')]
    if downloaded_files:
        latest_file = max(downloaded_files, key=lambda f: os.path.getmtime(os.path.join(download_directory, f)))
    # Get the full file path of the latest CSV file
        downloaded_file_path = os.path.join(download_directory, latest_file)
        #downloaded_filename = downloaded_files[0]  # Use the first CSV file in the directory
        #downloaded_file_path = os.path.join(download_directory, downloaded_filename)
        print(f'File downloaded successfully: {downloaded_file_path}')
    else:
        logging.error('Downloaded file not found or error in downloading.')
        raise FileNotFoundError("No CSV file found in the download directory.")

except Exception as e:
    logging.error(f"An error occurred: {e}")
finally:
    driver.quit()
    display.stop()

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("Connected to PostgreSQL successfully.")
except Exception as e:
    logging.error(f"Failed to connect to PostgreSQL: {e}")
    raise

# Function to clean column names
def clean_column_name(name):
    # Replace spaces with underscores and remove special characters
    name = re.sub(r'\s+', '_', name)  # Replace spaces with underscores
    name = re.sub(r'[^\w\s]', '', name)  # Remove special characters
    return name.lower() 

# Create database if it doesn't exist
cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
exists = cursor.fetchone()
if not exists:
    cursor.execute(f"CREATE DATABASE {DB_NAME};")
    print(f"Database {DB_NAME} created.")

# Load CSV into a DataFrame
df = pd.read_csv(downloaded_file_path)

# Create table name based on the CSV file name
table_name = latest_file.split('.')[0]  # Remove file extension

# Clean the column names
columns = [clean_column_name(col) for col in df.columns]
columns_str = ", ".join(columns)

# Create a table with columns based on the CSV columns
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id SERIAL PRIMARY KEY,
    {', '.join([f'{col} TEXT' for col in columns])}
);
"""
cursor.execute(create_table_query)
print(f"Table {table_name} created or already exists.")

# Insert data into the table
for index, row in df.iterrows():
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join([f'%s' for _ in columns])});"
    cursor.execute(insert_query, tuple(row))
    print(f"Inserted row {index + 1} into {table_name}")
    if index==10:
        break

# Close the connection
cursor.close()
conn.close()
print("Data insertion completed successfully.")

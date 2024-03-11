import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
from dotenv import load_dotenv, find_dotenv
from selenium.common.exceptions import NoSuchElementException, WebDriverException, ElementClickInterceptedException
from bs4 import BeautifulSoup
import numpy as np
import click
import os
import re
import warnings
warnings.filterwarnings('ignore')
import datetime


# Configuration Handling
def load_configuration():
    """
    Load configuration from environment variables or a .env file.
    """

    # Obtén el directorio de trabajo actual
    application_path = os.getcwd()

    # Construye la ruta al archivo .env
    dotenv_path = os.path.join(application_path, '.env')

    load_dotenv(dotenv_path)
    # Accessing variables
    userid = os.getenv('USER_CC1')
    password = os.getenv('PASSWORD_CC1')

    return userid, password


# WebDriver Setup
def setup_webdriver():
    """
    Initializes and returns a Selenium WebDriver with configured options.
    """
    base_path = r"/home/seluser/Downloads/ccc1/"
    current_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = os.path.join(base_path, current_date)

    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "profile.default_content_setting_values.notifications": 1,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "download.default_directory": folder_path,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_settings.popups":2,
    })
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
    return driver, folder_path

# Login Procedure
def login(driver, url, userid, password):
    """
    Logs into the website using provided credentials.
    
    :param driver: Selenium WebDriver instance
    :param url: URL to navigate to for login
    :param userid: User ID for login
    :param password: Password for login
    """
    driver.get(url)
    user_entry = driver.find_element(By.ID, "username")
    user_entry.send_keys(userid)
    password_entry = driver.find_element(By.ID, "password")
    password_entry.send_keys(password)
    login_button = driver.find_element(By.ID, "btnSave")
    login_button.click()

# Data Scraping and Processing
def scrape_and_process_data(driver, cartera, folder_path):
    """
    Navigates through the website, scrapes data, and saves it to a CSV file.
    
    :param driver: Selenium WebDriver instance
    :param cartera: The specific 'cartera' to scrape data for
    """
    data = []
    try:
        driver.switch_to.frame(1)
    except NoSuchElementException:
        print("The specified frame does not exist.")
        return
    except Exception as e:
        print(f"Unexpected error when switching to frame: {e}")
        return
    
    wait = WebDriverWait(driver, 30)
    
    try:
        list_cartera = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'select2-selection__rendered')))
        list_cartera.click()
        search_input = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "select2-search__field")))
        search_input.send_keys(cartera)
        driver.switch_to.active_element.send_keys(Keys.ENTER)
        #time.sleep(10)
    except TimeoutException:
        print("Timeout while trying to interact with the cartera selection.")
        return
    except Exception as e:
        print(f"Unexpected error during cartera selection: {e}")
        return

    max_retries = 3
    attempts = 0

    while attempts < max_retries:
        try:
            driver.get("https://app.ccc.uno/CallRecord")
            grabaciones = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="QFiltersCallStatus-CallRecord"]/label[4]/span')))
            break  # If the page loads and element is found, exit the loop
        except WebDriverException as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            if attempts == max_retries:
                print("Maximum retries reached. Exiting.")
                driver.quit()  # Quitting the driver as a last resort if all retries fail
                break
            print("Retrying...")
            time.sleep(5)  # Wait for 5 seconds before retrying
    else:
        print("Operation successful after", attempts, "attempts.")
    
    try:
        grabaciones.click()
        #time.sleep(30)  # Consider replacing with a more reliable waiting mechanism
    except TimeoutException:
        print("Timeout while trying to select grabaciones.")
        return

    time.sleep(5)
    #  Wait for the specific message to appear
    loading_message_locator = (By.ID, "divBackgridStatusText-CallRecord")
    expected_messages = ["Hay una Buscar en progreso, por favor espere hasta que se complete para empezar una nueva.",
                        "  Cargando ..."]

    # Attempt to locate the message element without waiting for a specific message
    try:
        message_element = driver.find_element(*loading_message_locator)
        current_message = message_element.text.strip()
        print("Message found: ", current_message)
        
        # If the specific loading message is present, then proceed with clicking the refresh button
        if current_message in expected_messages:
            print("Message indicating search in progress is present.")
            
            attempts = 0
            max_attempts = 5
            while attempts < max_attempts:
                try:
                    refresh_button = wait.until(EC.element_to_be_clickable((By.ID, "btnBackgridRefresh-CallRecord")))
                    refresh_button.click()
                    print("Refresh button clicked successfully.")
                    time.sleep(5)  # Consider replacing with more dynamic waiting

                    # Check for the message to change
                    new_message = driver.find_element(*loading_message_locator).text.strip()
                    if new_message not in expected_messages:
                        print("Message changed, indicating the page has loaded.")
                        break
                except WebDriverException as e:
                    print(f"Attempt {attempts + 1} failed: {e}")
                
                attempts += 1

            if attempts == max_attempts:
                print("Max attempts reached, moving on to data extraction.")
        else:
            print("No relevant loading message, proceeding with data extraction.")
            
    except NoSuchElementException:
        # If the message element isn't found at all, proceed with data extraction
        print("Loading message element not found, proceeding with data extraction.")
    except TimeoutException:
        # If waiting for the refresh button times out, handle accordingly
        print("Timeout while waiting for the refresh button, proceeding with caution.")

    while True:
        try: 
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".backgrid-paginator"))
            )
            table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'table-responsive')))
            # Obtener el contenido HTML de la tabla
            table_html = table.get_attribute("outerHTML")
            # Analizar la tabla con BeautifulSoup
            soup = BeautifulSoup(table_html, "html.parser")
            
            time.sleep(5)
            rows_ = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, 'tr')))
            for row in rows_:
                #driver.execute_script("document.body.style.zoom='50%'")
                buttons = row.find_elements(By.TAG_NAME, 'button')
                for button in buttons:
                    onclick = button.get_attribute('onclick')
                    if onclick and 'DownloadCallRecording' in onclick:
                        driver.execute_script("arguments[0].scrollIntoView();", button)
                        # Wait for button to be clickable
                        wait.until(EC.element_to_be_clickable((button)))
                        try:
                            button.click()
                        except ElementClickInterceptedException:
                            print("Button was not clickable.")
                            
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    rows = soup.find_all("tr")
                    
                    for row in rows:
                        cells = row.find_all("td")
                        row_data = [cell.text.strip() for cell in cells]
                        data.append(row_data) 
            if data:
                # Supongamos que las columnas de tu DataFrame tienen estos nombres
                column_names = ["ID", "Tipo", "Campaña", "Agente", "Origen", "Número", "Destino", "Estatus", "Duración", "Facturable", "Tarifa / Min", "Costo", "Botón1", "Botón2", "Inicio"]

                # Crear un DataFrame de Pandas
                df_pandas = pd.DataFrame(data, columns=column_names)

                # Nombre del archivo CSV (puedes cambiarlo según tus preferencias)
                csv_filename = 'datos_extraidos.csv'

                # Ruta completa del archivo CSV
                csv_filepath = os.path.join(folder_path, csv_filename)
                
                # Check if the CSV file already exists.
                if os.path.isfile(csv_filepath):
                    # Load the existing data, append the new data, and save
                    existing_df = pd.read_csv(csv_filepath)
                    combined_df = pd.concat([existing_df, df_pandas], ignore_index=True)
                    combined_df = combined_df.drop_duplicates()
                else:
                    # If the file doesn't exist, the new data is the comdined data
                    combined_df = df_pandas.drop_duplicates()
                # Guarda el DataFrame combinado en un archivo CSV
                combined_df.to_csv(csv_filepath, index=False)


                pass
            # Check if 'Next' button is not disabled, then click it, otherwise break the loop
            next_button = driver.find_elements(By.XPATH,"//li[not(contains(@class, 'disabled'))]/a[@title='Siguiente']")
            if next_button:
                next_button[0].click()
                time.sleep(2)  # Adjust based on your page's loading time
            else:
                break  # Exit the loop if no more pages to navigate      
                

        except TimeoutException:
            print("Timeout occurred while trying to process a page.")
            break
        except Exception as e:
            print(f"Unexpected error during page processing: {e}")
            break

    # Final steps, like closing the driver, can be here
    driver.quit()

def navigate_to_next_page(driver):
    """
    Attempts to navigate to the next page and returns True if successful, False otherwise.
    
    :param driver: Selenium WebDriver instance
    :param wait: WebDriverWait instance for handling waits
    """
    try: 
        next_button = driver.find_element(By.XPATH, '//a[@title="Siguiente"]/i[@class="fa fa-angle-right fa-lg"]')
        # Verificar si el botón está deshabilitado
        if "disabled" in next_button.find_element(By.XPATH, ('../..')).get_attribute("class"):
            return False # Next button is disabled, no more pages to process
        next_button.click()
        time.sleep(5)
    except NoSuchElementException:
        print("Next button not found, possibly at the last page.")
        return False
    except Exception as e:
        print(f"Unexpected error when navigating to the next page: {e}")
        return False

# File Management
def rename_downloaded_file(folder_path):
    """
    Removes files with a number in parentheses in their name, then renames audio files to keep only the last number.
    
    :param folder_path: The directory where the audio files are stored.
    """
    pattern = re.compile(r'\(\d+\)')  # Regular expression to match number in parentheses
    for filename in os.listdir(folder_path):
        full_path = os.path.join(folder_path, filename)

        # Check if the filename matches the pattern for removal first
        if  filename.endswith(".crdownload") or pattern.search(filename):
            os.remove(full_path)
            print(f"Removed file '{filename}'")
            continue  # Skip to the next file
        
        # Then check if it's an audio file that needs renaming
        try:
            if '-' in filename and filename.endswith(".wav"):
                new_name = filename.split('-')[-1]  # Extract the last number from the file name
                new_file = os.path.join(folder_path, new_name)
                
                # Check if the new file name already exists
                if os.path.exists(new_file):
                    os.remove(new_file)  # Delete the existing file
                    print(f"Deleted existing file '{new_name}' to make way for renaming")
                
                os.rename(full_path, new_file)
                print(f"Renamed '{filename}' to '{new_name}'")
        except Exception as e:
            print(f"Error processing file '{filename}': {e}")

# Main Workflow
def main(cartera):
    """
    Main workflow for scraping data, processing it, and sending an email.

    :param cartera: The specific 'cartera' to scrape data for
    """
    # Load configuration and capture returned values
    userid, password = load_configuration()
    driver, folder_path = setup_webdriver()
    login(driver, "https://app.ccc.uno/Campaign", userid, password)
    scrape_and_process_data(driver, cartera, folder_path)
    time.sleep(5)
    rename_downloaded_file(folder_path)
    # Assume additional steps for file handling and email sending here...
    driver.quit()

if __name__ == "__main__":
    # The calls to main and other functions are commented out to prevent execution in this environment
    main(2803)
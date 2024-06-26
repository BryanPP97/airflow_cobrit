from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from datetime import date
from zoneinfo import ZoneInfo
import datetime
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from utils.utils_general import get_positive
import json
import warnings
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
warnings.filterwarnings('ignore')

def gepard_automation():
    load_dotenv(find_dotenv()) # Load the .env file.
    userid = os.getenv("USER_ID")
    passwo = os.getenv("USER_PASSWORD")

    url = "https://www.message-center.com.mx/"
    # Configuración para evitar notificaciones
    chrome_options = Options()
    #chrome_options.add_argument('--headless')
    #chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    "profile.default_content_settings.popups":2,
    "download.default_directory":"/home/seluser/Downloads/gepard"
    })
    
    # Configuración para ingresar al explorador
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
    #driver = webdriver.Chrome(options = chrome_options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)

        # Configurar credenciales
    user = driver.find_element(By.ID,'Usr')
    user.send_keys(userid)
    password = driver.find_element(By.ID, 'Pass')
    password.send_keys(passwo)
    ingresar = driver.find_element(By.CLASS_NAME, 'cssButtonMn')
    ingresar.click()
    time.sleep(10)

        # Ir a mensajes
    mensajes = driver.find_element(By.XPATH, '//*[@id="MenuPanel"]/table/tbody/tr/td[2]/img')
    mensajes.click()
    time.sleep(5)
        # Agregar datos
    buscar = driver.find_element(By.CLASS_NAME, 'cssBtn')
    buscar.click()
        # Exportar Excel
    export = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'img[title="Exportar a Excel..."]')))
    export.click()
    time.sleep(15)
    driver.quit()

def process_sms():
    page = "Gepard"
    path = '/opt/airflow/outputs/gepard/Resultados.csv'

    # This is the code to do the get_positive function

    ## Get last business day
    today = date.today()
    #today = datetime.datetime.now(ZoneInfo("America/Mexico_City"))
    yesterday = ( today - pd.tseries.offsets.BDay(0) ).normalize()
    print("Today:", yesterday)
    #df_today
    df = pd.read_csv(path)
    df.Fecha = pd.to_datetime( df.Fecha )
    df_sorted = df.sort_values('Fecha', ascending=True)  ## Was False
    df_sorted['normalised_date'] = df_sorted['Fecha'].dt.normalize()
    df_today = df_sorted.loc[df_sorted.normalised_date == yesterday]
    df_today.drop(columns=['normalised_date', 'Fecha'], inplace=True)
    ## Read json File to get names
    with open('/opt/airflow/files/jsonFile.json') as f:
        maps = f.read()
        parsed_json = json.loads(maps)
    ## Iterates over names
    for key in parsed_json.keys():
        cartera = key
        words = parsed_json.get(key)['words']
        get_positive(df_today, page, cartera, words)


def generate_gepard_filename(**context):
    """
    Renames the original file to a new name based on the DAG's execution date.
    The new filename and path are saved in XCom for other tasks to access.
    """
    # Get the execution date of the DAG from the context
    execution_date = context['ds']
    
    # Path and original name of the file 
    original_file_path = '/opt/airflow/outputs/gepard/Resultados.csv'  # Example
    
    # Format the execution date to construct the new filename
    new_filename = f"mensajes_gepard_{execution_date}.csv"
    new_file_path = f"/opt/airflow/outputs/gepard/{new_filename}"
    
    # Rename  the original file to the new path with the new name
    os.rename(original_file_path, new_file_path)
    
    # Save the new filename and path in XCom for other tasks to access
    context['ti'].xcom_push(key='new_file_path', value=new_file_path)
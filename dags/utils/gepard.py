from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from datetime import date
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from utils.utils_general import get_positive
import json
import warnings
warnings.filterwarnings('ignore')

def gepard_automation():
    load_dotenv(find_dotenv()) # Load the .env file.
    userid = os.getenv("USER_ID")
    passwo = os.getenv("USER_PASSWORD")

    url = "https://www.message-center.com.mx/"
    # Configuración para evitar notificaciones
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    "profile.default_content_settings.popups":0,
    "download.default_directory":"/opt/airflow/outputs/Gepard/"
    })
    # Configuración para ingresar al explorador
    driver = webdriver.Chrome(options = chrome_options)
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
    time.sleep(10)
    driver.quit()

def process_sms():
    page = "Gepard"
    path = '/opt/airflow/outputs/Gepard/Resultados.csv'
    
    ## Get last business day
    today = date.today()
    yesterday = ( today - pd.tseries.offsets.BDay(1) ).normalize()

    df = pd.read_csv(path)
    df.Fecha = pd.to_datetime( df.Fecha )
    df_sorted = df.sort_values('Fecha', ascending=False)
    df_sorted['normalised_date'] = df_sorted['Fecha'].dt.normalize()
    df_today = df_sorted.loc[df_sorted.normalised_date ==yesterday]
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
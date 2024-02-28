from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import zipfile
import os
from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains
import warnings
warnings.filterwarnings('ignore')

def ift_automation():
    # Configura las opciones de Chrome para la descarga
    #download_folder = "/opt/airflow/outputs/ift"
    chrome_options = Options()
    #chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        #"download.default_directory": download_folder,  # Utiliza la ubicación actual del script como carpeta de descarga
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True, # Desactiva la verificación de seguridad de descargas
        "profile.default_content_settings.popups":0,
        "profile.default_content_setting_values.automatic_downloads": 2,
    })

    # Configuración para ingresar al explorador
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
    url = "https://sns.ift.org.mx:8081/sns-frontend/planes-numeracion/descarga-publica.xhtml"
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    time.sleep(10)
    button = wait.until(EC.element_to_be_clickable((By.ID, "FORM_planes:BTN_planPublico1")))
    button.send_keys(Keys.ENTER)

    # Espera un tiempo para que la descarga se complete
    tiempo_espera = 60  # Puedes ajustar este valor según sea necesario
    tiempo_transcurrido = 0
    archivo_zip = None

    # Tu código existente para esperar a que el archivo ZIP aparezca...
    while tiempo_transcurrido < tiempo_espera:
        archivos_en_carpeta = os.listdir("/opt/airflow/outputs/")
        for archivo in archivos_en_carpeta:
            if archivo.endswith('.zip'):
                archivo_zip = archivo
                break

        if archivo_zip:
            break

        time.sleep(1)
        tiempo_transcurrido += 1

    # Modificación para renombrar el archivo CSV extraído a 'ift.csv'
    if archivo_zip:
        with zipfile.ZipFile(os.path.join("/opt/airflow/outputs/", archivo_zip), 'r') as zf:
            # Extrae todos los archivos dentro del archivo ZIP
            zf.extractall("/opt/airflow/outputs/")
            
            # Lista los archivos extraídos para encontrar el archivo CSV
            for archivo in zf.namelist():
                if archivo.endswith('.csv'):
                    archivo_csv_original = archivo
                    break
            
            # Ruta completa del archivo CSV original
            ruta_archivo_csv_original = os.path.join("/opt/airflow/outputs/", archivo_csv_original)
            
            # Ruta completa del nuevo nombre del archivo CSV
            ruta_nuevo_archivo_csv = os.path.join("/opt/airflow/outputs/", "ift.csv")
            
            # Renombra el archivo CSV extraído a 'ift.csv'
            os.rename(ruta_archivo_csv_original, ruta_nuevo_archivo_csv)

    driver.quit()
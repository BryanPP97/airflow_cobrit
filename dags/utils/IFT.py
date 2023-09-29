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
warnings.filterwarnings('ignore')

def ift_automation():
    # Configura las opciones de Chrome para la descarga
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        "download.default_directory": "/opt/airflow/outputs/Gepard/",  # Utiliza la ubicación actual del script como carpeta de descarga
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False, # Desactiva la verificación de seguridad de descargas
        "profile.default_content_settings.popups":0,
    })

    # Configuración para ingresar al explorador
    driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub", options=chrome_options)
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

    while tiempo_transcurrido < tiempo_espera:
        archivos_en_carpeta = os.listdir("/opt/airflow/outputs/IFT/")
        for archivo in archivos_en_carpeta:
            if archivo.endswith('.zip'):
                archivo_zip = archivo
                break

        if archivo_zip:
            break

        time.sleep(1)
        tiempo_transcurrido += 1

    if archivo_zip:
        # Abre el archivo ZIP en modo de lectura
        with zipfile.ZipFile(os.path.join("/opt/airflow/outputs/IFT/", archivo_zip), 'r') as zf:
            # Extrae todos los archivos y carpetas dentro del archivo ZIP en la carpeta de destino
            zf.extractall("/opt/airflow/outputs/IFT/")

    driver.quit()

ift_automation()

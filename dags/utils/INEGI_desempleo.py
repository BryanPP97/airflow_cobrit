from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import shutil
from datetime import datetime
from selenium.webdriver.common.action_chains import ActionChains
import requests
import os

def INEGI_desempleo_scraper():

    

    # Ensure the download directory exists

    url = "https://www.inegi.org.mx/app/tabulados/default.html?nc=624"
    
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        "download.default_directory": "/opt/airflow/outputs/INEGI/",  # Set to the dynamically determined path
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "profile.default_content_settings.popups": 0,
        "profile.default_content_setting_values.automatic_downloads": 2,
    })
    # Configuración para ingresar al explorador
    #driver = webdriver.Chrome(options = chrome_options)
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
    driver.get(url)
    wait= WebDriverWait(driver, 10)
    button = wait.until(EC.element_to_be_clickable((By.ID, "aCsv")))
    button.click()
    time.sleep(20)
<<<<<<< HEAD
    print("Se extrae tabulado")
=======
    print("Aquí se descarga")
>>>>>>> 2a4754d59266d979b185e803eff1f5fd7e595391
    driver.quit()
    
def data_transformation():
    path_archivo = "/opt/airflow/outputs/Tabulado.csv"

    # Ajusta los permisos del archivo antes de leerlo
    #os.chmod(path_archivo, 0o666)  # Establece los permisos a rw-rw-rw-

    df = pd.read_csv(path_archivo)

    # Encuentra los índices donde comienzan los años en la columna 'Periodo'
    indices_anios = df[df['Periodo'].str.isnumeric()].index
    nuevo_df = df.T.iloc[1:].reset_index(drop=True)
    nuevo_df = nuevo_df.dropna(axis =1)
    num_columnas = len(nuevo_df.columns)
        
    years = range(2020,2025)
    # Lista de números romanos
    romanos = ['I', 'II', 'III', 'IV']
    # Crea una lista de nombres de columnas basados en el patrón
    nombres_columnas = [f"{anno}-{romano}" for anno in years for romano in romanos]
    nombres_columnas = nombres_columnas[:num_columnas]
    #Asigna los nombres de las columnas al nuevo DataFrame
    nuevo_df.columns = nombres_columnas 
    # Agrega una columna 'Estado' al nuevo DataFrame
    nuevo_df.insert(0, 'Estado', df.columns[1:])

    # Restablece el índice
    nuevo_df.reset_index(drop=True, inplace=True)
    nuevo_df.to_csv(path_archivo, index = False)

    #data_transformation()
    
#INEGI_desempleo_scraper()
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from datetime import datetime, timedelta, date
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from utils.utils_general import get_positive
import json
import warnings
warnings.filterwarnings('ignore')

def marcatel_automation():
    url = "https://tink.marcatel.com.mx/Login.aspx"
    
    load_dotenv(find_dotenv()) # Load the .env file.
    userid = os.getenv("USER_MARCATEL")
    passwo = os.getenv("PASSWORD_MARCATEL")
   
    # Configuración para evitar notificaciones
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    #"profile.default_content_settings.popups":0,
    "download.default_directory":"/opt/airflow/outputs/Marcatel/"})
    # Configuración para ingresar al explorador
    driver = webdriver.Chrome(options = chrome_options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    user = wait.until(EC.presence_of_element_located((By.ID, "usuario")))
    user.click()
    user.send_keys(userid)
    contra = wait.until(EC.presence_of_element_located((By.ID, "contrasena")))
   
    # Pasar credenciales
    contra.click()
    contra.send_keys(passwo)
    init_button = wait.until(EC.element_to_be_clickable((By.ID, "btnini")))
    init_button.click()
    time.sleep(30)
    # Abrir ventana de reportes SMS
    # Redirecciona a la URL deseada
    driver.get("https://tink.marcatel.com.mx/Reportes/wfReporteRespuestas.aspx")
    # Espera unos segundos antes de cerrar el navegador
    time.sleep(30)

    reporte = wait.until(EC.element_to_be_clickable((By.ID, "generarReporte")))
    reporte.click()
    time.sleep(30)
    exportar = driver.find_element(By.ID, "excel")
    driver.execute_script("arguments[0].scrollIntoView();", exportar)
    exportar.click()
    time.sleep(10)
    driver.quit()

def process_sms():
    page = "Marcatel"
    i_path = "/opt/airflow/outputs/Marcatel/"
    
    ## Get last business day
    #today = date.today()
    #yesterday = ( today - pd.tseries.offsets.BDay(1) ).normalize()
    #yesterday = pd.to_datetime(today).normalize()
    ###

    for file in os.listdir(i_path):
        if file.endswith(".xlsx"):
            d_path = os.path.join(i_path, file)
            print(d_path)
    df = pd.read_excel(d_path, usecols=["Telefono", "MensajeRespuesta", "NombreEnvio"])#, "FechaRespuesta"])
    df.rename(columns={"NombreEnvio":"Proyecto"}, inplace=True) #"FechaRespuesta":"Fecha"

    #df.Fecha = pd.to_datetime( df.Fecha )
    #df_sorted = df.sort_values('Fecha', ascending=False)
    #df_sorted['normalised_date'] = df_sorted['Fecha'].dt.normalize()
    #df_today = df_sorted.loc[df_sorted.normalised_date ==yesterday]
    #df_today.drop(columns=['normalised_date', 'Fecha'], inplace=True)
    ## Read json File to get names
    with open('/opt/airflow/files/jsonFile.json') as f:
        maps = f.read()
        parsed_json = json.loads(maps)
    ## Iterates over names
    for key in parsed_json.keys():
        cartera = key
        words = parsed_json.get(key)['words']
        get_positive(df, page, cartera, words) #df_today
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import click
import os
import shutil
from datetime import datetime, timedelta, date
import yagmail
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from utils.utils_nlp1 import *
import pyodbc
import warnings
warnings.filterwarnings('ignore')

def process_sms(page):
    if page == 'Gepard':
        path = '/opt/airflow/outputs/Gepard/Resultados.csv'
    elif page == 'Marcatel':
        path = '/opt/airflow/outputs/Resultados.csv'  #### Change this!!!!!
    else:
        return None
    today = date.today()
    yesterday = pd.to_datetime( today - timedelta(days = 1) ).normalize()

    df = pd.read_csv(path)
    df.Fecha = pd.to_datetime( df.Fecha )
    df_sorted = df.sort_values('Fecha', ascending=False)
    df_sorted['normalised_date'] = df_sorted['Fecha'].dt.normalize()
    df_today = df_sorted.loc[df_sorted.normalised_date ==yesterday]
    df_today.drop(columns=['normalised_date', 'Fecha'], inplace=True)

    cartera = "DIMEX"
    get_positive(df_today, cartera, page)

def email():
    correos = ['ia_1@coperva.net', 'jmontan@coperva.com']
    mensaje = "Buen día\nLes comparto las cuentas que tienen prioridad para el día de hoy\n\nSaludos,\nAlberto Montán."
    dmx_path = '/opt/airflow/outputs/Gepard/DIMEX.csv'
    rappi_path = '/opt/airflow/outputs/rappi.csv'
    send_email(correos, mensaje, dmx_path)
    #send_email(correos, mensaje, rappi_path)

def send_email(receivers, body, filename):
    load_dotenv(find_dotenv()) # Load the .env file.
    password = os.getenv("ACCOUNT_PASSWORD")
    yag = yagmail.SMTP("jmontan@coperva.com", password)
    yag.send(
        to=receivers,
        subject="Cuentas con priorirdad",
        contents=body, 
        attachments=filename,
    )

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
    "download.default_directory":"/opt/airflow/outputs/Marcatel/"
})
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
    ###
    html = driver.page_source
    print(html)
    ###
    # Abrir ventana de reportes SMS
    # Redirecciona a la URL deseada
    driver.get("https://tink.marcatel.com.mx/Reportes/wfReporteRespuestas.aspx")
    # Espera unos segundos antes de cerrar el navegador
    time.sleep(30)

    reporte = wait.until(EC.element_to_be_clickable((By.ID, "generarReporte")))
    reporte.click()
    time.sleep(30)
    ###
    html = driver.page_source
    print(html)
    ###
    current_url = driver.current_url
    print("URL actual:", current_url)
    ###
    exportar = driver.find_element(By.ID, "excel")
    driver.execute_script("arguments[0].scrollIntoView();", exportar)
    exportar.click()
    time.sleep(10)
    driver.quit()

def run_query(query, cnxn_str=None):
    """
    query: String query
    cnxn_str: Conection to server in string
    
    data: Result of query as DataFrame
    
    This function creates the conection with the server,
    runs the query and return the result as a pandas Data Framne
    """
    if cnxn_str is None:
        ## Default conection to majestic server
        cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=majestic.jezapps.com;"
                "DATABASE=coperva_ia;"
                "UID=iateam;"
                "PWD=Coperva2023$$;"
                "Trusted_Connection=no"
               )
    cnxn = pyodbc.connect(cnxn_str)
    data = pd.read_sql(query, cnxn)
    cnxn.close()
    return data

def get_positive(df, cartera, portal):
    save_paht = f'/opt/airflow/outputs/{portal}/'
    df_0 = df.copy()
    df_0[cartera] = df_0.Proyecto.apply(lambda X: 1 if cartera in X else 0)
    
    df_1 = pd.read_csv("/opt/airflow/files/Spanish-NRC-EmoLex.csv")
    df_1.rename(columns={'anger':'enojo', 'anticipation':'anticipacion', 'disgust':'disgusto', 
                  'fear':'miedo', 'joy':'disfrutar', 'negative':'negativo',
       'positive':'positivo', 'sadness':'tristeza', 'surprise':'sorpresa', 
                  'trust':'confianza', 'Spanish Word':'token'}, inplace=True)
    
    df_2 = df_0.loc[df_0[cartera] == 1]
    df_2.rename(columns={'Mensaje': 'MensajeRespuesta'}, inplace=True)
    dff=genera_token(df_2, 'MensajeRespuesta')
    dft=dff.merge(df_1, how='left', on='token')
    dft=dft.fillna(0)
    d=dft.groupby(by=['Telefono','MensajeRespuesta' , 'Proyecto']).sum()
    posi=pd.DataFrame(d[(d['positivo']>0) & (d['enojo']==0)&(d['positivo']>d['negativo'])])
    ####
    posi.to_csv(f"{save_paht}{cartera}.csv", index=False)
    ####
    if posi.shape[0] > 0:
        telefonos = posi.reset_index().Telefono.unique()
        phones = "("
        for tel in telefonos:
            phones = phones + f"'{tel}',"
        phones = phones[:-1] + ")"
        query = (f"""
        SELECT CUENTA, tel
        FROM [dbo].[TB_TELEFONOS]
        WHERE CLIENTE = 'RAPPI' 
        AND tel in {phones}
        """)
        #result = run_query(query)
        #result.to_csv(f"{save_paht}{cartera}.csv", index=False)
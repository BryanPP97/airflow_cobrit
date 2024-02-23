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
import json
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import warnings
from bs4 import BeautifulSoup
import json
import csv
from urllib.parse import urlparse
from selenium.common.exceptions import WebDriverException

warnings.filterwarnings('ignore')

def obtener_nombre_estado(url):
    parsed_url = urlparse(url)
    path = parsed_url.path
    parts = path.split("/")
    if len(parts) >= 3:
        estado = parts[2].replace("-", " ")
        # Tratar la excepción de "distrito-federal"
        if "estado-de" in estado:
            estado = estado.replace("estado-de-", "")
        elif "distrito-federal" in estado:
            estado = "ciudad de mexico"
        return estado
    return None

def clima_scraper():
    max_attempts = 4
    attempts = 0

    while attempts < max_attempts:
        datos_climaticos = []
        try:

            urls =  [
            "https://www.clima.com/mexico/estado-de-aguascalientes/aguascalientes",
            "https://www.clima.com/mexico/estado-de-baja-california/mexicali",
            "https://www.clima.com/mexico/estado-de-baja-california-sur/la-paz",
            "https://www.clima.com/mexico/estado-de-campeche/campeche",
            "https://www.clima.com/mexico/estado-de-chiapas/tuxtla-gutierrez",
            "https://www.clima.com/mexico/estado-de-chihuahua/chihuahua",
            "https://www.clima.com/mexico/distrito-federal/mexico",
            "https://www.clima.com/mexico/estado-de-coahuila-de-zaragoza/saltillo",
            "https://www.clima.com/mexico/estado-de-colima/colima",
            "https://www.clima.com/mexico/estado-de-durango/victoria-de-durango",
            "https://www.clima.com/mexico/estado-de-guanajuato/guanajuato",
            "https://www.clima.com/mexico/estado-de-guerrero/chilpancingo-de-los-bravos",
            "https://www.clima.com/mexico/estado-de-hidalgo/pachuca-de-soto",
            "https://www.clima.com/mexico/estado-de-jalisco/guadalajara",
            "https://www.clima.com/mexico/estado-de-mexico/ecatepec",
            "https://www.clima.com/mexico/estado-de-michoacan-de-ocampo/morelia",
            "https://www.clima.com/mexico/estado-de-morelos/cuernavaca",
            "https://www.clima.com/mexico/estado-de-nayarit/tepic",
            "https://www.clima.com/mexico/estado-de-nuevo-leon/monterrey",
            "https://www.clima.com/mexico/estado-de-oaxaca/oaxaca-de-juarez",
            "https://www.clima.com/mexico/estado-de-puebla/puebla-de-zaragoza",
            "https://www.clima.com/mexico/estado-de-queretaro-de-arteaga/queretaro",
            "https://www.clima.com/mexico/estado-de-quintana-roo/cancun-2",
            "https://www.clima.com/mexico/estado-de-san-luis-potosi/san-luis-potosi",
            "https://www.clima.com/mexico/estado-de-sinaloa/culiacan",
            "https://www.clima.com/mexico/estado-de-sonora/hermosillo",
            "https://www.clima.com/mexico/estado-de-tabasco/villahermosa",
            "https://www.clima.com/mexico/estado-de-tamaulipas/reynosa",
            "https://www.clima.com/mexico/estado-de-tlaxcala/tlaxcala",
            "https://www.clima.com/mexico/estado-de-veracruz-llave/veracruz",
            "https://www.clima.com/mexico/estado-de-yucatan/merida",
            "https://www.clima.com/mexico/estado-de-zacatecas/zacatecas"
        ]
        
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_experimental_option("prefs", {
            "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
            "profile.default_content_settings.popups":0,
            "download.default_directory":"/opt/airflow/outputs/clima/"
            })
            
            # Configuración para ingresar al explorador
            remote_webdriver = 'remote_chromedriver'
            driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)



            for url in urls:
                driver.get(url)
                wait = WebDriverWait(driver, 60)

                container= wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "table")))
                container.location_once_scrolled_into_view

                desplazamiento_vertical = 400  # Cambia esto a la cantidad de píxeles que desees desplazarte

                # Realiza el desplazamiento
                driver.execute_script(f"window.scrollBy(0, {desplazamiento_vertical});")

                time.sleep(10)

                #driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                page_soruce = driver.page_source
                soup = BeautifulSoup(page_soruce, "html.parser")
                # Encontrar el elemento <canvas>
                canvas = soup.find('canvas', class_='chart gtm-climateAverage-chart')

                # Obtener el valor del atributo 'data-months'
                data_months = canvas['data-months']

                
                months_data = json.loads(data_months)
                # Obtener el nombre del estado
                nombre_estado = obtener_nombre_estado(url)

                for mes in months_data:
                    mes['Estado'] = nombre_estado # Agregar la nueva columna con el nombre del estado

                datos_climaticos.extend(months_data)

                # Crear un archivo CSV y escribir los datos
                with open('/opt/airflow/outputs/clima/datos_climaticos.csv', mode='w', newline='') as file:
                    fieldnames = list(months_data[0].keys())  # Obtener los nombres de las columnas
                    #fieldnames.append('Estado')  # Agregar la nueva columna 'Estado'
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(datos_climaticos)

            driver.quit()
            break
        except WebDriverException as e:
            print(f"Se produjo una excepción: {e}")
            attempts += 1
            print(f"Reintentando (Intento {attempts}/{max_attempts})...")
            driver.quit()
            continue
    if attempts == max_attempts:
        print(f"Se alcanzó el número máximo de intentos")
        driver.quit()
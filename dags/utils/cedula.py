import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import numpy as np
from unidecode import unidecode
import warnings
import pyautogui
warnings.filterwarnings('ignore')
from utils_general import *


def cedula_scraper():
    
    df_input = name_read()
    
    # Set selenium options and url
    url = 'https://www.cedulaprofesional.sep.gob.mx/cedula/presidencia/indexAvanzada.action'
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
    "profile.default_content_setting_values.notifications": 1,
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    "plugins.always_open_pdf_externally": True,
    "download.default_directory":"/opt/airflow/outputs/cedula/"
    })
    # Configuración para ingresar al explorador
    driver = webdriver.Chrome(options = chrome_options)
    driver.get(url)
    wait = WebDriverWait(driver, 20)
    
    time.sleep(10)

    # Scraper
    data=[]
    #total_data_downloaded = 0
    for index, row in df_input.iterrows():
        nombre_persona = row['Nombre_Completo']
    
        # Se escrolea hasta la parte específica 
        driver.execute_script("window.scrollTo(10, 7600);")
        time.sleep(5)
        
        # Se ingresan las variables del scrapeo
        nombre=row['Nombre']
        paterno=row['Paterno']
        materno=row['Materno']
        
        # Ingresar nombres
        celda_nombre = driver.find_element(By.ID, 'nombre')
        celda_nombre.clear()
        celda_nombre.send_keys(str(nombre))

        #Ingresar Apellido Paterno
        celda_paterno = driver.find_element(By.ID, 'paterno')
        celda_paterno.clear()
        celda_paterno.send_keys(str(paterno))

        # Ingresar Apellido Materno
        celda_materno = driver.find_element(By.ID, 'materno')
        celda_materno.clear()
        celda_materno.send_keys(str(materno))

        # Click en botón buscar
        buscar = driver.find_element(By.ID, 'dijit_form_Button_0_label')
        buscar.click()
        time.sleep(2)

        max_wait_time = 5  # Tiempo máximo de espera en segundos
        start_time = time.time()  # Momento de inicio de espera

        while time.time() - start_time < max_wait_time:
            try:
                alert = driver.switch_to.alert
                alert.accept()
                driver.refresh()
                break  # Salir del bucle si se encuentra la ventana emergente
            except:

                time.sleep(1)  # Esperar 1 segundo antes de intentar nuevamente

        time.sleep(5)
        
        # Esperar a que los elementos estén visibles
        ver_botones = WebDriverWait(driver, 10).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'ver-mas')))
        html = driver.page_source
        print(html)

        
        for ver in ver_botones:
            nombre_boton = ver.get_attribute("value")
            print(nombre_boton)
            if unidecode(nombre_boton.lower()) == unidecode(nombre_persona.lower()):
                cantidad_registros = len(ver_botones)
                #print("Cantidad de registros:", cantidad_registros)

                #time.sleep()
                ver.click()
                
                # Encontrar los elementos de entrada de texto por su id
                input_cedula = driver.find_element(By.ID, "input-cedula-result")
                input_tipo = driver.find_element(By.ID, "input-tipo-result")
                input_sexo = driver.find_element(By.ID, "input-sexo-result")
                input_nombre = driver.find_element(By.ID, "input-nombre-result")
                input_paterno = driver.find_element(By.ID, "input-paterno-result")
                input_materno = driver.find_element(By.ID, "input-materno-result")
                input_escuela = driver.find_element(By.ID, "input-escuela-result")
                input_titulo = driver.find_element(By.ID, "input-titulo-result")

                # Obtener los valores de los campos
                valor_cedula = input_cedula.get_attribute("value")
                valor_tipo = input_tipo.get_attribute("value")
                valor_sexo = input_sexo.get_attribute("value")
                valor_nombre = input_nombre.get_attribute("value")
                valor_paterno = input_paterno.get_attribute("value")
                valor_materno = input_materno.get_attribute("value")
                valor_escuela = input_escuela.get_attribute("value")
                valor_titulo = input_titulo.get_attribute("value")

                # Agregar los valores a la lista de diccionarios
                data.append({
                "Cédula": valor_cedula,
                "Tipo de Cédula": valor_tipo,
                "Sexo": valor_sexo,
                "Nombre": valor_nombre,
                "Paterno": valor_paterno,
                "Materno": valor_materno,
                "Escuela": valor_escuela,
                "Título": valor_titulo
                })
                # Encontrar los elementos de entrada de texto por su id
                cerrar = driver.find_element(By.XPATH, '//*[@id="cerrar_resut_personal"]')
                cerrar.click()
            
    driver.quit()            
    df_data = pd.DataFrame(data)
    print(df_data)
    
    #df_data.to_csv('Z:/Data/HSBC/SCRAPPING/Cédulas/0_.csv')
    
    driver.quit()
    
if __name__ =='__main__':
    cedula_scraper()
    
    
    
    
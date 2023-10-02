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
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import numpy as np
from unidecode import unidecode
import warnings
warnings.filterwarnings('ignore')
from utils_general import *


def cedula_scraper():
    
    init=193
    final=500

    df = pd.read_excel('Z:/Data/HSBC/Asignaciones/ASG_TDC_IA_ENERO_JUNIO.xlsx')
    df['Nombre'] = df.Nombre.apply(lambda X: X.replace('/', ' ').replace('*', '').rstrip())
    df_filtered = df['Nombre']
    df_filtered.drop_duplicates(inplace=True)
    df_input = pd.DataFrame(df_filtered.reset_index(drop=True)).iloc[init:final]



    df_input['NombreSplit'] = df_input.Nombre.apply(lambda X: split_name(X))
    df_input['Nombre'] = df_input.NombreSplit.apply(lambda X: X[0])
    df_input['Paterno'] = df_input.NombreSplit.apply(lambda X: X[1])
    df_input['Materno'] = df_input.NombreSplit.apply(lambda X: X[2])

    # Concatenación
    df_input['Nombre_Completo'] = df_input.apply(lambda row: ' '.join([row['Nombre'], row['Paterno'], row['Materno']]), axis = 1)
    
    # Set selenium options and url
    url = 'https://cedulaprofesionalsep.online/#Consulta_de_Cedula_Profesional'
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
    
    #iframe = wait.until(EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, 'body')))
    #iframe.click()
    time.sleep(10)
    #driver.switch_to.frame(iframe)
    
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
        celda_nombre = driver.find_element(By.XPATH, '//*[@id="input-nombre"]')
        celda_nombre.clear()
        celda_nombre.send_keys(str(nombre))

        #Ingresar Apellido Paterno
        celda_paterno = driver.find_element(By.XPATH, '//*[@id="input-apaterno"]')
        celda_paterno.clear()
        celda_paterno.send_keys(str(paterno))

        # Ingresar Apellido Materno
        celda_materno = driver.find_element(By.XPATH, '//*[@id="input-amaterno"]')
        celda_materno.clear()
        celda_materno.send_keys(str(materno))

        # Click en botón buscar
        buscar = driver.find_element(By.XPATH, '//*[@id="container-form-1"]/form/div[4]/div/input')
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
    df_data.to_csv('Z:/Data/HSBC/SCRAPPING/Cédulas/0_.csv')
    
    driver.quit()
    
if __name__ =='__main__':
    cedula_scraper()
    
    
    
    
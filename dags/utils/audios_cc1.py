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
import click
import os
import re
import warnings
warnings.filterwarnings('ignore')

## 1752 IZZI
#@click.command()
#@click.option('--cartera', prompt='Ingresa el número de la cartera', help='La cartera a obtener Audios')
def cc1_automation(cartera):
    load_dotenv(find_dotenv()) # Load the .env file.
    url = "https://app.ccc.uno/Campaign"

    # Set selenium options and url
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
    "profile.default_content_setting_values.notifications": 1,
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    "plugins.always_open_pdf_externally": True,
    "download.default_directory":"/opt/airflow/audios"
    })
    # Configuración para ingresar al explorador
    #driver = webdriver.Chrome(options = chrome_options)
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
    
    driver.get(url)
    wait = WebDriverWait(driver, 10)

    userid = os.getenv("USER_CC1")
    user_entry = driver.find_element(By.ID, "username")
    user_entry.send_keys(userid)
    password = os.getenv("PASSWORD_CC1")
    password_entry = driver.find_element(By.ID, "password")
    password_entry.send_keys(password)
    login = driver.find_element(By.ID, "btnSave")
    login.click()
    driver.switch_to.frame(1)
    
    list_cartera = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'select2-selection__rendered')))
    list_cartera.click()
    search_input = wait.until(
    EC.presence_of_element_located((By.CLASS_NAME, "select2-search__field"))
)
    
    search_input.send_keys(cartera)
    driver.switch_to.active_element.send_keys(Keys.ENTER)

    historial = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="side-nav"]/li[6]')))
    historial.click()
    time.sleep(30)
    grabaciones = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="QFiltersCallStatus-CallRecord"]/label[4]/span')))
    grabaciones.click()
    time.sleep(30) 
    df = []

   # Inicializar una variable para contar las páginas
    page_number = 1

    # Buscar el botón de "Siguiente" por título y clase
    next_button = driver.find_element(By.XPATH, '//a[@title="Siguiente"]/i[@class="fa fa-angle-right fa-lg"]')

    while True:
        table = wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'table-responsive')))
        # Obtener el contenido HTML de la tabla
        table_html = table.get_attribute("outerHTML")
        # Analizar la tabla con BeautifulSoup
        soup = BeautifulSoup(table_html, "html.parser")

        # Obtener los nombres de las columnas desde los elementos <th> en el encabezado de la tabla
        column_names = [th.text for th in soup.find_all("th")]
        
        data = []

        # Obtener las filas después de hacer clic en "Siguiente"
        rows_ = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, 'tr')))
        rows = soup.find_all("tr")
        labels = []
        for row in rows_:
            buttons = row.find_elements(By.TAG_NAME, 'button')
            for button in buttons:
                onclick = button.get_attribute('onclick')
                if onclick and 'DownloadCallRecording' in onclick:
                    driver.execute_script("arguments[0].scrollIntoView();", button)
                    time.sleep(10)
                    button.click()
        for row in rows:
            cells = row.find_all("td")
            row_data = [cell.text.strip() for cell in cells]
            data.append(row_data)        
            encabezado = driver.find_elements(By.TAG_NAME, 'th')
            labels.append(row.text)
            datos = driver.find_elements(By.TAG_NAME, 'td')
        


        # Supongamos que las columnas de tu DataFrame tienen estos nombres
        column_names = ["ID", "Tipo", "Campaña", "Agente", "Origen", "Número", "Destino", "Estatus", "Duración", "Facturable", "Tarifa / Min", "Costo", "Botón1", "Botón2", "Inicio"]

        # Crear un DataFrame de Pandas
        df_pandas = pd.DataFrame(data, columns=column_names)

        # Imprimir el DataFrame
        print(df_pandas)
        # Directorio donde deseas guardar el archivo CSV
        save_directory = "/opt/airflow/audios"

        # Nombre del archivo CSV (puedes cambiarlo según tus preferencias)
        csv_filename = 'datos_extraidos.csv'

        # Ruta completa del archivo CSV
        csv_filepath = os.path.join(save_directory, csv_filename)

        # Guardar el DataFrame en un archivo CSV
        df_pandas.to_csv(csv_filepath, index=False)  # El argumento index=False evita que se guarde el índice en el CSV

        
        # Verificar si el archivo CSV ya existe
        if os.path.isfile(csv_filepath):
            # Si el archivo existe, carga el CSV existente en un DataFrame
            existing_df = pd.read_csv(csv_filepath)
            
            # Concatena el DataFrame existente con el nuevo DataFrame df_pandas
            combined_df = pd.concat([existing_df, df_pandas], ignore_index=True)
        else:
            # Si el archivo no existe, simplemente usa df_pandas
            combined_df = df_pandas

        # Guarda el DataFrame combinado en un archivo CSV
        combined_df.to_csv(csv_filepath, index=False)

        print(f"DataFrame guardado en: {csv_filepath}")


        # Verificar si el botón está deshabilitado
        is_disabled = "disabled" in next_button.find_element(By.XPATH, ('../..')).get_attribute("class")
        if is_disabled:
            print("Botón 'Siguiente' deshabilitado. Has llegado al final de las páginas.")
            break
        
        next_button.click()
        time.sleep(5)
        # Actualizar el botón "Siguiente" para la próxima iteración
        next_button = driver.find_element(By.XPATH, '//a[@title="Siguiente"]/i[@class="fa fa-angle-right fa-lg"]')
        
        # Incrementar el número de página
        page_number += 1
    
    # Cierra el navegador
    driver.quit()
    def rename_downloaded_file(file_extension):
        # Directorio donde deseas guardar los archivos
        save_directory = r'Z:\AUDIOS\BANORTE'
        
        # Directorio donde se descargaron los archivos (cambia esto según tu caso)
        download_directory = r'C:\Users\DSTHREE\Downloads'

        # Encuentra el archivo descargado (puedes ajustar el patrón según la descarga)
        for filename in os.listdir(download_directory):
            if filename.endswith(file_extension):
                # Utiliza una expresión regular para encontrar el último grupo de números después del cuarto guión
                match = re.search(r'[^-]+$', filename)
                
                if match:
                    # Obtiene el resultado de la coincidencia (el último grupo de números)
                    ultimo_grupo_numerico = match.group(0)
                    
                    # Construye la ruta completa del nuevo archivo con el nuevo nombre
                    new_filepath = os.path.join(save_directory, ultimo_grupo_numerico)

                    # Construye la ruta completa del archivo descargado actual
                    current_filepath = os.path.join(download_directory, filename)

                    # Mueve el archivo a la ubicación deseada y renómbralo
                    os.rename(current_filepath, new_filepath)
    #Renombra el archivo de descarga
    #rename_downloaded_file(".wav")

#if __name__ == '__main__':
#    cc1_automation()

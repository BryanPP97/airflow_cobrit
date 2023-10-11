from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import time
import re
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
import os
import pandas as pd
import json
import warnings
warnings.filterwarnings('ignore')


def gobernadores_scraper():
    url = 'https://www.conago.org.mx/gobernadores'
    

   
    # Configuración para evitar notificaciones
    chrome_options = Options()
    #options.add_argument("window-size=1200x600")
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    #"profile.default_content_settings.popups":0,
    "download.default_directory":"/opt/airflow/outputs/Gobernadores/"})
    # Configuración para ingresar al explorador

    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
    driver.get(url)

    # Localiza los elementos de interés
    elementos = driver.find_elements(By.CLASS_NAME, 'col-xs-12.col-md-8.texto.rcGobernadores')


    # Crear o abrir el archivo CSV
    with open('datos_1.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Nombre', 'Fecha de Inicio', 'Fecha de Fin', 'Estado']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()  # Escribir la fila de encabezados
        
        for _ in range(len(elementos)):
            # Obtén el elemento específico y el estado
            elemento = elementos[_]
            estado_element = elemento.find_element(By.CLASS_NAME, 'textMedio')
            estado = estado_element.text
            estado_element.click()  # Hacer clic en el enlace
            
            # Obtener los elementos de interés en la página de detalle del estado
            elementos_estado = driver.find_elements(By.CLASS_NAME, 'col-xs-12.col-sm-6.col-md-4.pb15')

            for elemento_estado in elementos_estado:
                nombre_gobernador = elemento_estado.find_element(By.TAG_NAME, 'h4').text
                html = elemento_estado.get_attribute('innerHTML')
                
                # Usar expresión regular para encontrar las fechas
                fechas = re.search(r'(\d{2}/\d{2}/\d{4}) a (\d{2}/\d{2}/\d{4})', html)
                
                if fechas:
                    fecha_inicio = fechas.group(1)
                    fecha_fin = fechas.group(2)
                else:
                    fecha_inicio = "Fecha no disponible"
                    fecha_fin = "Fecha no disponible"
                
                # Imprimir para verificar
                print(nombre_gobernador, fecha_inicio, fecha_fin)
                
                # Escribir los datos en el archivo CSV
                writer.writerow({'Nombre': nombre_gobernador, 'Fecha de Inicio': fecha_inicio,
                                'Fecha de Fin': fecha_fin, 'Estado': estado})

            driver.back()  # Regresar a la página anterior

        # Cerrar el navegador
        driver.quit()
gobernadores_scraper()



def partidos_scraper():
    # Configura el controlador de Chrome
    driver = webdriver.Chrome()

    # Abre la página web de Wikipedia
    url = 'https://es.wikipedia.org/wiki/Wikipedia:Portada'
    driver.get(url)

    # Leer los nombres de los gobernadores desde el archivo CSV
    nombres_gobernadores = []
    resultados = []

    with open('datos.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            nombre_completo = row['Nombre']
            
            # Buscar el último punto en el nombre
            ultimo_punto_index = nombre_completo.rfind('.')
            if ultimo_punto_index != -1:
                nombre_completo = nombre_completo[ultimo_punto_index+1:].strip()
            
            nombres_gobernadores.append(nombre_completo)

    # ... (código anterior) ...

    # Iterar sobre los nombres y buscar en la página web
    for nombre in nombres_gobernadores:
        buscar_gobernador = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'cdx-text-input__input'))
        )

        # Usar ActionChains para borrar y escribir
        actions = ActionChains(driver)
        actions.click(buscar_gobernador).key_down(Keys.CONTROL).send_keys('a').key_up(Keys.CONTROL).send_keys(Keys.DELETE).send_keys(nombre).perform()
        buscar_gobernador = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'cdx-text-input__input'))
        )
        buscar_gobernador.submit()
        buscar_gobernador = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'cdx-text-input__input'))
        )
        # Esperar a que se cargue la página del artículo
        driver.implicitly_wait(10)

        # Verificar si la página muestra resultados de búsqueda
        try:
            no_results_heading = driver.find_element(By.CLASS_NAME, 'firstHeading.mw-first-heading')
            if no_results_heading.text == 'Resultados de la búsqueda':
                print(f"No se encontró información para {nombre}")
                print("---")
                continue
        except NoSuchElementException:
            pass

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        infobox = soup.find('table', {'class': 'infobox biography vcard'})

        if infobox:
            partido_element = infobox.find('th', string='Partido político')
            if partido_element:
                partido_td = partido_element.find_next('td')
                partido = partido_td.get_text(strip=True)

                # Buscar el año dentro de los paréntesis
                anio_inicio_element = partido_td.find('span', {'style': 'font-size:85%;'})
                if anio_inicio_element:
                    anio_inicio = anio_inicio_element.get_text(strip=True).strip('()')
                else:
                    anio_inicio = "No disponible"

                resultados.append({"Nombre": nombre, "Partido político": partido, "Año de inicio": anio_inicio})
            else:
                resultados.append({"Nombre": nombre, "Partido político": "No disponible", "Año de inicio": "No disponible"})
        else:
            resultados.append({"Nombre": nombre, "Partido político": "No disponible", "Año de inicio": "No disponible"})

    # Guardar los resultados en un archivo CSV
    with open('resultados_gobernadores.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["Nombre", "Partido político", "Año de inicio"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(resultados)

    # Cerrar el navegador
    driver.quit()
partidos_scraper()

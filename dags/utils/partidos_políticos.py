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


def partidos_scraper():
    # Configura el controlador de Chrome
    driver = webdriver.Chrome()

    # Abre la página web de Wikipedia
    url = 'https://es.wikipedia.org/wiki/Wikipedia:Portada'
    chrome_options = Options()
    #chrome_options.add_argument('--headless')
    #chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    "profile.default_content_settings.popups":0,
    "download.default_directory":"/opt/airflow/outputs/partidos_politicos/"
    })
    
    # Configuración para ingresar al explorador
    remote_webdriver = 'remote_chromedriver'
    driver = webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options)
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
        time.sleep(5)
        buscar_gobernador.send_keys(Keys.ENTER)
        #button_search = WebDriverWait(driver, 10).until((EC.element_to_be_clickable((By.CLASS_NAME, 'cdx-button cdx-button--action-default cdx-button--weight-normal cdx-button--size-medium cdx-button--framed cdx-search-input__end-button'))))
        #button_search.click()
        #buscar_gobernador.submit()
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


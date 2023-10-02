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


def redes_sociales_scraper():
    nombre = '''Bryan Antonio Polito Palma'''
    
    url = 'https://duckduckgo.com/'
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
    #"download.default_directory":"C:/Users/DSTHREE/Documents/GITHUB/SCRAPER_VARIABLES"
})
    driver = webdriver.Chrome(options = chrome_options)
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    search_input = wait.until(EC.presence_of_element_located((By.ID, 'searchbox_input')))
    search_input.send_keys(nombre)
    time.sleep(10)
    search = driver.find_element(By.XPATH, "//button[@aria-label='Search']")
    search.click()
    driver.execute_script("window.scrollTo(10, 7600);")
    mas_resultados =   driver.find_element(By.ID, 'more-results')
    driver.execute_script("arguments[0].scrollIntoView();", mas_resultados)
    # Encuentra todos los elementos 'li' con el atributo 'data-layout="organic"'
    elementos_li = driver.find_elements(By.CSS_SELECTOR,'li[data-layout="organic"]')

    # Itera a través de los elementos y extrae el texto
    for elemento_li in elementos_li:
        texto = elemento_li.text
        print(texto)
    time.sleep(10)
    #html = driver.page_source
    #print(html)
    driver.quit()
    

if __name__ == '__main__':
    redes_sociales_scraper()
    

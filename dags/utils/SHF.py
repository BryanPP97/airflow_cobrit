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
import PyPDF2



def ift_scraper():
    url = "https://www.gob.mx/shf/documentos/indice-shf-de-precios-de-la-vivienda-en-mexico-2021-a-2025?state=published"
    
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
    "download.prompt_for_download": False,  # Desactiva la ventana emergente de descarga
    "download.directory_upgrade": True,
    "safebrowsing.enabled": False,  # Desactiva la verificación de seguridad de descargas
})
    # Configuración para ingresar al explorador
    driver = webdriver.Chrome(options = chrome_options)
    driver.get(url)
    wait= WebDriverWait(driver, 10)
    
    # Encuentra todos los elementos li con la clase "clearfix documents"
    document_elements = driver.find_elements(By.CSS_SELECTOR, "li.clearfix.documents")
    
    # Verifica si hay al menos un elemento
    if document_elements:
        # Encuentra el primer elemento de la lista
        first_document = document_elements[0]
        
        # Encuentra botón de descarga
        pdf_link = first_document.find_element(By.CSS_SELECTOR, "a[href$='.pdf']")
        
        # Obtiene el enlace del atributo href
        pdf_url = pdf_link.get_attribute("href")
        
        # Abre el enlace en una nueva ventana
        driver.execute_script("window.open('" + pdf_url + "', '_blanck');")
        
        # Cambia el enfoque a la nueva ventana
        driver.switch_to.window(driver.window_handles[-1])
        
        # Obtener URL actual
        url_actual = driver.current_url
        
        # Ruta destino
        ruta_destino = "SHF.pdf"
        
        # Realiza la solicitud HTTP para descargar el archivo
        response=requests.get(url_actual)
        
        # Verifica si la descarga fue exitosa (código de estado 200)
        if response.status_code == 200:
            # Abre un archivo en modo binario y guarda el contenido del PDF
            with open(ruta_destino, 'wb') as pdf_file:
                pdf_file.write(response.content)
    driver.quit()


    def pdf_convert():
        pdf_file = "SHF.pdf"
        
        # Abre el archivo PDF
        pdf_file = open(pdf_file, 'rb')

        # Crea un objeto PdfFileReader para leer el PDF
        pdf_reader = PyPDF2.PdfReader(pdf_file)

        # Inicializa una lista para almacenar el texto de cada página
        page_texts = []

        # Itera a través de las páginas y extrae el texto
        for page in pdf_reader.pages:
            page_texts.append(page.extract_text())

        # Cierra el archivo PDF
        pdf_file.close()

        # Combina el texto de todas las páginas en un solo texto
        all_text = '\n'.join(page_texts)

        # Divide el texto en líneas
        lines = all_text.split('\n')

        # Crea un DataFrame a partir de las líneas
        df = pd.DataFrame(lines, columns=['Texto'])

        ruta_csv = "SHF.csv"    
        df.to_csv(ruta_csv, index=False)
        return df

    def extractor_tablas():
        df = pd.read_csv("SHF.csv")
        df = df.drop(df.index[48:])
        # Reajusta el índice para que comience desde 0
        df = df.reset_index(drop=True)
        # Elimina la primera fila, que contiene el encabezado, y guarda el resultado en un nuevo DataFrame
        df_sin_encabezado = df.iloc[1:].copy()

        # Extrae la columna 'Valores'
        df_sin_encabezado['Valores'] = df_sin_encabezado['Texto'].str.extract(r'(\d[\d. \s]+)')
        # Dividir la columna 'Valores' en múltiples columnas usando espacios en blanco como separadores
        columnas_valores = df_sin_encabezado['Valores'].str.split(expand=True)
        # Eliminar filas que contienen valores NaN (en este caso, la fila que contiene "Na")
        columnas_valores = columnas_valores.dropna()

        df_sin_encabezado['Estado'] = (df_sin_encabezado['Texto'].str.extract('([^0-9]+)'))
        # Combina la columna 'Estado' de df_sin_encabezado con columnas_valores
        columnas_valores.insert(0, 'Estado', df_sin_encabezado['Estado'].values)
        # Acceder a la segunda columna por posición
        segunda_columna = columnas_valores.iloc[:, 1]
        # Lista de índices de filas a modificar 
        filas_a_modificar = [0, 33, 34]

        for fila_idx in filas_a_modificar:
            # Modificar los valores de la segunda columna
            segunda_columna.iloc[fila_idx] = str(segunda_columna.iloc[fila_idx])[1:]

        columnas_valores = columnas_valores.rename(columns={columnas_valores.columns[0]: 'Estado'})


        #Obtener la longitud 
        num_columnas = len(columnas_valores.columns)
        # Crear una lista de años desde 2015 hasta 2024
        years = range(2015, 2025)
        # Crear una lista de números romanos del I al IV
        romanos = ['I', 'II', 'III', 'IV']
        # Crea una lista de nombres de columna basados en el patrón deseado
        nombres_columnas = ["Estado"] + [f"{anno}-{romano}" for anno in years for romano in romanos]
        # Asegurarse de que la lista de nombres de columna tenga la misma longitud que la tabla
        nombres_columnas = nombres_columnas[:num_columnas]
        columnas_valores.columns = nombres_columnas
        # Guardar el DataFrame modificado en un archivo CSV
        columnas_valores.to_csv("SHF_extract.csv", index=False)

    pdf_convert()
    extractor_tablas()
    
ift_scraper()

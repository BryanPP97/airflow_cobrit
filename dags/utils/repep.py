from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.alert import Alert
import time
import pandas as pd



def repep_scraper():
    repep = []
    for telefono in telefonos :
        chrome_options = Options()
        # Configuraciones adicionales omitidas para brevedad

        driver = webdriver.Chrome(options=chrome_options)
        url = "https://repep.profeco.gob.mx/Solicitudnumero.jsp"
        driver.get(url)
        wait = WebDriverWait(driver, 10)
            
        # Encuentra el elemento y realiza las acciones
        input_repep = driver.find_element(By.ID, "telefono")
        input_repep.click()  # Primero haz click en el elemento
        input_repep.send_keys(telefono)  # Luego envía las teclas
        input_repep.submit()  # Finalmente, realiza el envío del formulario
        time.sleep(5)
        try : 
            #Esperar por la vetnana emergente durante 10 minutos
            WebDriverWait(driver, 10).until(EC.alert_is_present())
            #Cambiar el enfoque a la vetnana emergente
            alert = driver.switch_to.alert
            # Obtener el texto de la ventana emergente
            alert_text = alert.text
            print(f'Alerta encontrada:{alert_text}')
            alert.accept()
        except TimeoutException:
            # Si no se encuentra ninguna ventana emergente en 10 segundos
            print("No se encontró la ventana emergente.")
            repep.append(telefono)
            print(repep)
    
        driver.quit()
    repep_df = pd.DataFrame(repep, columns=['Telefono'])
    repep_df.to_csv('repep.csv', index=False)
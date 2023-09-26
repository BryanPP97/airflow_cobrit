import os
import shutil
import yagmail
from dotenv import load_dotenv, find_dotenv
import pandas as pd
from utils.utils_nlp1 import *
import pyodbc
import json
import warnings
warnings.filterwarnings('ignore')

def clean_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

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
        cnxn_str = ("Driver={ODBC Driver 18 for SQL Server};"
                    "Server=majestic.jezapps.com;"
                    "DATABASE=coperva_ia;"
                    "UID=iateam;"
                    "PWD=Coperva2023$$;"
                    "Trusted_Connection=no;"
                    "TrustServerCertificate=yes"
                    )
        
    print(pyodbc.drivers())
    cnxn = pyodbc.connect(cnxn_str)
    data = pd.read_sql(query, cnxn)
    cnxn.close()
    return data

def get_positive(df, portal, cartera, words):
    save_paht = f'/opt/airflow/outputs/{portal}/'
    ## Busca por las palabras
    df_0 = df.copy()
    df_0[cartera] = 0
    for word in words:
        temp = df_0.Proyecto.apply(lambda X: 1 if word in X else 0)
        df_0[cartera] = df_0[cartera] + temp
    
    ## Se carga datos para obtener positivos
    df_1 = pd.read_csv("/opt/airflow/files/Spanish-NRC-EmoLex.csv")
    df_1.rename(columns={'anger':'enojo', 'anticipation':'anticipacion', 'disgust':'disgusto', 
                  'fear':'miedo', 'joy':'disfrutar', 'negative':'negativo',
       'positive':'positivo', 'sadness':'tristeza', 'surprise':'sorpresa', 
                  'trust':'confianza', 'Spanish Word':'token'}, inplace=True)
    
    df_2 = df_0.loc[df_0[cartera] > 0]
    df_2.rename(columns={'Mensaje': 'MensajeRespuesta'}, inplace=True)
    dff=genera_token(df_2, 'MensajeRespuesta')
    dft=dff.merge(df_1, how='left', on='token')
    dft=dft.fillna(0)
    d=dft.groupby(by=['Telefono','MensajeRespuesta' , 'Proyecto']).sum()
    posi=pd.DataFrame(d[(d['positivo']>0) & (d['enojo']==0)&(d['positivo']>d['negativo'])])
    if posi.shape[0] > 0:
        telefonos = posi.reset_index().Telefono.unique()
        phones = "("
        for tel in telefonos:
            phones = phones + f"'{tel}',"
        phones = phones[:-1] + ")"
        query = (f"""
        SELECT CUENTA, tel
        FROM [dbo].[TB_TELEFONOS]
        WHERE CLIENTE = '{cartera}'
        AND tel in {phones}
        """)
        result = run_query(query)
        if result.shape[0] > 0:
            result.to_csv(f"{save_paht}{cartera}.csv", index=False)

def email(portal):
    cpath = f'/opt/airflow/outputs/{portal}/'
    mensaje = "Buen día\nLes comparto las cuentas que tienen prioridad para el día de hoy\n\nSaludos,\nAlberto Montán."
    ## Read json File to get names
    with open('/opt/airflow/files/jsonFile.json') as f:
        maps = f.read()
        parsed_json = json.loads(maps)
    ## Iterates over names
    for key in parsed_json.keys():
        cartera = key
        correos = parsed_json.get(key)['emails']
        cc = parsed_json.get(key)['cc']
        path_cartera = f"{cpath}{cartera}.csv"
        if os.path.exists(path_cartera):
            send_email(correos, cc, mensaje, path_cartera, cartera)

def send_email(receivers, cc, body, filename, cartera):
    load_dotenv(find_dotenv()) # Load the .env file.
    password = os.getenv("ACCOUNT_PASSWORD")
    yag = yagmail.SMTP("jmontan@coperva.com", password)
    yag.send(
        to=receivers,
        cc=cc,
        subject=f"Cuentas con priorirdad {cartera}",
        contents=body, 
        attachments=filename,
    )
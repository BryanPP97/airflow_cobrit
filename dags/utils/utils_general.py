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
            final = add_message(result, df_2)
            final.CUENTA = final.CUENTA.astype(str)
            final.to_excel(f"{save_paht}{cartera}.xlsx", index=False)

def add_message(result, original):
    df_1 = result.copy()
    df_2 = original.copy()
    smss = list()

    for i, row in df_1.iterrows():
        #print(row['tel'])
        phone = int(row['tel'])
        #print(df_2.loc[ df_2.Telefono == phone ])
        message = df_2.loc[ df_2.Telefono == phone ].MensajeRespuesta.values[0]
        smss.append(message)
    df_1["Mensaje"] = smss
    return df_1

def email(portal):
    cpath = f'/opt/airflow/outputs/{portal}/'
    mensaje = "Buen día\nLes comparto las cuentas con respuesta positiva.\n\nSaludos,\nAlberto Montán."
    ## Read json File to get names
    with open('/opt/airflow/files/jsonFile.json') as f:
        maps = f.read()
        parsed_json = json.loads(maps)
    ## Iterates over names
    for key in parsed_json.keys():
        cartera = key
        correos = parsed_json.get(key)['emails']
        cc = parsed_json.get(key)['cc']
        path_cartera = f"{cpath}{cartera}.xlsx"
        if os.path.exists(path_cartera):
            send_email(correos, cc, mensaje, path_cartera, cartera)

def send_email(receivers, cc, body, filename, cartera):
    load_dotenv(find_dotenv()) # Load the .env file.
    password = os.getenv("ACCOUNT_PASSWORD")
    yag = yagmail.SMTP("jmontan@coperva.com", password)
    yag.send(
        to=receivers,
        cc=cc,
        subject=f"Respuestas SMS {cartera}",
        contents=body, 
        attachments=filename,
    )

def split_name(name):
    """
    name: complete name to split

    output: list of name splited [name, paterno, materno]
    """
    name = name.replace('*','').replace('/',' ').replace('-',' ').rstrip()
    splited = get_name(name)
    if len(splited)==1:
        return [splited[0], None, None]
    if len(splited) <3:
        nam1 = splited[0]
        nam2 = splited[1]
        return [nam1, nam2, None]
    if len(splited) <4:
        nam1 = splited[0]
        nam2 = splited[1]
        nam3 = splited[2]
        return [nam1, nam2, nam3]
    nam1 = ' '.join(splited[:-2])
    nam2 = splited[-2]
    nam3 = splited[-1]
    return [nam1, nam2, nam3]
def get_name(name, inverse=False):
    """
    Support function for split_name()

    name: name to split
    inverse: True when name init with last name

    returns:
    List of name divided (last name can include 'de', 'los', etc.)
    """
    flag=0
    skips = ['de', 'los', 'la', 'santa', 'del']
    namedir = name.lower().split(' ')
    new_name = list()
    for i in range(len(namedir)):
        word = namedir[i]
        if flag:
            temp.append(namedir[i-1])
        if word in skips:
            if not flag:
                temp = list()
            flag=1
            continue
        if flag:
            temp.append(namedir[i])
            word = ' '.join(temp)
        new_name.append(word)
        flag = 0
    return new_name

def name_read():
    """
    Read and preprocess names from an Asignation file.

    Reads a specific range of names from an Excel file, preprocesses the names by removing special characters,
    and splits them into first names, paternal last names, maternal last names, and full names.

    Returns:
        DataFrame: A DataFrame containing the preprocessed names and their components.
    """
    
    init = 190
    final = 200
    df = pd.read_excel('Z:/Data/HSBC/Asignaciones/ASG_TDC_IA_ENERO_JUNIO.xlsx')
    df['Nombre'] = df.Nombre.apply(lambda X: X.replace('/', ' ').replace('*', '').rstrip())
    df_filtered = df['Nombre']
    df_filtered.drop_duplicates(inplace = True)
    df_input = pd.DataFrame(df_filtered.reset_index(drop = True)).iloc[init:final]
    
    df_input['NombreSplit'] = df_input.Nombre.apply(lambda X: split_name(X))
    df_input['Nombre'] = df_input.NombreSplit.apply(lambda X: X[0])
    df_input['Paterno'] = df_input.NombreSplit.apply(lambda X: X[1])
    df_input['Materno'] = df_input.NombreSplit.apply(lambda X: X[2])
    
    df_input['Nombre_Completo'] = df_input.apply(lambda row: ' '.join([row['Nombre'], row['Paterno'], row['Materno']]), axis = 1)
    df_input.to_csv('df_input.csv')
    return df_input
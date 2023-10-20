import pandas as pd
import re
from unidecode import unidecode
import pyodbc



def normalize_string(s):
    return unidecode(s.strip().lower())

def process_dataframe(df, columns):
    for col in columns:
        df[col] = df[col].apply(normalize_string)
    return df


def gobernadores():
    df_gob = pd.read_csv("C:/Users/DSTHREE/Documents/GITHUB/airflow/dags/utils/datos.csv")
    nuevo_registro = pd.DataFrame({
    "Nombre": ["Alfredo del Mazo Maza"],
    "Fecha de Inicio": ["16/09/2017"],
    "Fecha de Fin": ["15/09/2023"],
    "Estado": ["mexico"]
    })

    df_gob = pd.concat([df_gob, nuevo_registro]).reset_index(drop=True)

    df_gob['Nombre'] = df_gob['Nombre'].str.replace(r'\w+\.', '', regex=True)
    df_gob['Nombre'] = df_gob['Nombre'].str.replace(r'\s', '', regex=True)
    df_gob['Estado'] = df_gob['Estado'].str.lower()
    df_gob['Estado'] = df_gob['Estado'].apply(unidecode)

    return df_gob

def pp_gob():

    df_pp = pd.read_csv("C:/Users/DSTHREE/Documents/GITHUB/airflow/dags/utils/resultados_gobernadores.csv")
    nuevo_registro = pd.DataFrame({
        "Nombre" : ['Alfredo del Mazo Maza','Layda Elena Sansores San Román', 'Rutilio Cruz Escandón Cadenas','Esteban Alejandro Villegas Villarreal', 'Evelyn Cecia Salgado Pineda', 'Enrique Alfaro Ramírez', 'Sergio Salomón Céspedes Peregrina', 'María Elena Lezama Espinosa', 'José Ricardo Gallardo Cardona', 'Francisco Alfonso Durazo Montaño'],
        "Partido político": ["Partido Revolucionario Institucional", 'Movimiento Regeneración Nacional','Movimiento Regeneración Nacional', "Partido Revolucionario Institucional", 'Movimiento Regeneración Nacional', 'Movimiento Ciudadano', 'Movimiento Regeneración Nacional', 'Movimiento Regeneración Nacional', 'Partido Verde Ecologista de México', 'Movimiento Regeneración Nacional'],
        "Año de inicio": ['No disponible', 'No disponible','No disponible', 'No disponible', 'No disponible', 'No disponible', 'No disponible', 'No disponible', 'No disponible', 'No disponible'],
    })

    df_pp = pd.concat([df_pp, nuevo_registro]).reset_index(drop=True)
    df_pp['Nombre'] = df_pp['Nombre'].str.replace(r'\s', '', regex=True)

    def split_parties(row):
        party = None
        year = None

        match = re.search(r'(.+?)\(([^)]+)\)', row)
        if match:
            party = match.group(1)
            years_in_match = re.findall(r'\d{4}', match.group(2))
            if years_in_match:
                year = max(int(year) for year in years_in_match)
        
        return party, year


    # Aplicar la función a cada fila
    split_data = df_pp['Partido político'].apply(split_parties)
    df_pp['Partido'] = split_data.apply(lambda x: x[0])
    df_pp['Año'] = split_data.apply(lambda x: x[1])
    df_pp['Partido'].fillna(df_pp['Partido político'], inplace=True)

    df_pp= df_pp[['Nombre', 'Partido']]

    return df_pp

def shf():
    df_shf = pd.read_csv('C:/Users/DSTHREE/Documents/GITHUB/airflow/dags/utils/SHF_extract.csv')
    df_shf = df_shf.drop(df_shf.index[33:])
    df_shf = df_shf.melt(id_vars=['Estado'], var_name = 'Trimestre', value_name='SHF')
    # Usar str.contains para filtrar los registros que contienen "2023"
    df_shf = df_shf[df_shf['Trimestre'].str.contains('2023')]
    df_shf['Estado'] = df_shf['Estado'].str.lower()
    df_shf

    return df_shf

def desempleo():
    
    df_desemp = pd.read_csv('C:/Users/DSTHREE/Documents/GITHUB/airflow/dags/utils/Tabulado.csv')
    df_desemp = df_desemp.melt(id_vars=['Estado'], var_name='Trimestre', value_name='DP')

    renombre = {
        'Coahuila de Zaragoza' : 'Coahuila',
        'Distrito Federal' : 'Ciudad de México',
        'Michoacán de Ocampo' : 'Michoacán',
        'Veracruz de Ignacio de la Llave':'Veracruz'
    }

    df_desemp['Estado']=df_desemp['Estado'].replace(renombre)
    df_desemp['Estado'] = df_desemp['Estado'].str.lower() 
    df_desemp['Estado'] = df_desemp['Estado'].apply(unidecode)
    return df_desemp


def clima_dp_shf(df_shf):
    df_clima = pd.read_csv('C:/Users/DSTHREE/Documents/GITHUB/airflow/dags/utils/datos_climaticos.csv')
    df_clima['Estado'] = df_clima['Estado'].str.replace(r'^estado de ', '', regex=True)
    renombre = {
        'coahuila de zaragoza' : 'coahuila',
        'distrito federal' : 'ciudad de mexico',
        'michoacan de ocampo' : 'michoacan',
        'veracruz llave':'veracruz',
        'queretaro de arteaga': 'queretaro',
    }

    df_clima['Estado']=df_clima['Estado'].replace(renombre)


    meses_a_trimestres = {
        'Enero': 'I',
        'Febrero': 'I',
        'Marzo': 'I',
        'Abril': 'II',
        'Mayo': 'II',
        'Junio': 'II',
        'Julio': 'III',
        'Agosto': 'III',
        'Septiembre': 'III',
        'Octubre': 'IV',
        'Noviembre': 'IV',
        'Diciembre': 'IV'
    }

    # Agregar una columna de Trimestre al DataFrame de meses
    df_clima['Trimestre'] = df_clima['name'].map(meses_a_trimestres)
    df_clima['Trimestre'] = '2023-' + df_clima['Trimestre']

    # Fusionar DataFrames en función del Estado y el Trimestre
    df_dp_clima = df_clima.merge(df_desemp, on=['Estado', 'Trimestre'])
    # Quitar espacios y acentos para df_dp_clima
    df_dp_clima['Estado'] = df_dp_clima['Estado'].str.strip().apply(unidecode)

    # Quitar espacios y acentos para df_shf
    df_shf['Estado'] = df_shf['Estado'].str.strip().apply(unidecode)

    # Ahora intenta hacer el merge nuevamente
    df_shf_all = df_dp_clima.merge(df_shf, on=['Estado', 'Trimestre'], how='inner')
    return df_shf_all

def pp(df_gob_pp):
    df_gob_pp['Fecha de Inicio'] = pd.to_datetime(df_gob_pp['Fecha de Inicio'], dayfirst=True)
    df_gob_pp['Fecha de Fin'] = pd.to_datetime(df_gob_pp['Fecha de Fin'], dayfirst=True)

    meses_a_trimestres_english = {
    'January': 'I',
    'February': 'I',
    'March': 'I',
    'April': 'II',
    'May': 'II',
    'June': 'II',
    'July': 'III',
    'August': 'III',
    'September': 'III',
    'October': 'IV',
    'November': 'IV',
    'December': 'IV'
    }

    def obtener_trimestres(inicio, fin):
        trimestres = []
        
        while inicio <= fin:
            año = inicio.year
            mes = inicio.month
            
            trimestre = meses_a_trimestres_english[str(inicio.strftime('%B'))]

    # Obtener trimestre a partir del mes
            trimestres.append(f"{año}-{trimestre}")
            
            if mes >= 10:
                inicio = pd.Timestamp(year=inicio.year + 1, month=1, day=1)
            else:
                inicio = pd.Timestamp(year=inicio.year, month=inicio.month + 3, day=1)
        
        return trimestres

    df_gob_pp['Trimestres'] = df_gob_pp.apply(lambda row: obtener_trimestres(row['Fecha de Inicio'], row['Fecha de Fin']), axis=1)
    df_gob_pp = df_gob_pp.explode('Trimestres', ignore_index=True)
    df_gob_pp.rename(columns={'Trimestres': 'Trimestre'}, inplace=True)

    return df_gob_pp



def save_to_sql(df, table_name, cnxn_str=None, if_exists='replace', index=False):
    """
    df: DataFrame to be saved
    table_name: String name of the table in the database
    cnxn_str: Connection string to the server
    if_exists: {'fail', 'replace', 'append'}, default 'replace'
        - fail: If table exists, do nothing.
        - replace: If table exists, drop it, recreate it, and insert data.
        - append: If table exists, insert data. Create if does not exist.
    index: bool, default False
        Write DataFrame index as a column.

    This function saves a DataFrame to a SQL Server database.
    """

    if cnxn_str is None:
        # Default connection to majestic server
        cnxn_str = ("Driver={ODBC Driver 18 for SQL Server};"
                    "Server=majestic.jezapps.com;"
                    "DATABASE=coperva_ia;"
                    "UID=iateam;"
                    "PWD=Coperva2024$$;"
                    "Trusted_Connection=no;"
                    "TrustServerCertificate=yes"
                    )

    # Create a connection
    cnxn = pyodbc.connect(cnxn_str)

    # Save the DataFrame to SQL
    df.to_sql(table_name, cnxn, if_exists=if_exists, index=index)

    # Close the connection
    cnxn.close()



# Use the function
if __name__ == "__main__":
    
    # Gobernadores
    df_gob = gobernadores()
    df_gob = process_dataframe(df_gob, ['Nombre', 'Estado'])

    # Partidos Políticos
    df_pp = pp_gob()
    df_pp = process_dataframe(df_pp, ['Nombre'])
    
    # SHF
    df_sfh = shf()
    df_sfh = process_dataframe(df_sfh, ['Estado'])
    
    # Desempleo
    df_desemp = desempleo()
    df_desemp = process_dataframe(df_desemp, ['Estado'])
    
    # Unificar datos
    df_gob_pp = pd.merge(df_pp, df_gob, on='Nombre')
    df_shf_all = clima_dp_shf(df_sfh)
    df_gob_pp = pp()
    df_final = df_shf_all.merge(df_gob_pp[['Estado', 'Trimestre', 'Partido']], on=['Estado', 'Trimestre'], how='left')
    
    # Guardar resultados
    df_final.to_csv('variables_externas.csv')
    save_to_sql(df_gob, 'df_final')
# -*- coding: utf-8 -*-
"""
Created on Mon Sep 11 16:50:25 2023

@author: DSTWO
"""
import re
import unicodedata
import nltk
nltk.download('stopwords')



# Transformar el texto a minuscula 

def minuscula(texto):
    return texto.lower()

#Remover URL 


def remover_url(texto):
    url = re.compile(r'https?://\S+|www\.\S+')
    return url.sub(r'', texto)
#Remover Usuarios (''@usuario') 
#Si aparecen usuarios combinados ('@usuario__usuario') queda '_usuario' y la funcion remover_no_alfabeto(texto) lo elimina

def remover_usuario(texto):
    text = re.sub(r"\@[A-Za-z0-9]+", "", texto)
    return text
# Remover los tags HTML 

def remover_tag_html(texto):
    html = re.compile(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    return re.sub(html, '', texto)

#remove tildes...
def remover_acento(texto):
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return texto



#Remover Puntos 
def remover_punto(texto):
    import string
    texto = ''.join([c for c in texto if c not in string.punctuation])
    return texto

#Remover Numeros 
def remover_numero(texto):
    texto = ''.join([i for i in texto if not i.isdigit()])
    return texto

#Remover espacios en Blanco (extras/tabs) 
def remover_espacio_extra(texto):
    import re
    pattern = r'^\s*|\s\s*'
    return re.sub(pattern, ' ', texto).strip()

#Remueve todo lo que se sea del alfabeto 
def remover_no_alfabeto(texto):
    return ' '.join([i for i in texto.split() if i.isalpha() == True])

# remove palabras sin sentido artículos,.... 

def remove_stopwords(texto):    
    stopwords=set(nltk.corpus.stopwords.words("spanish"))
    for i in stopwords:
        texto = re.sub(r"\b%s\b" % i, " ", texto)
    return texto

#Lematizar







# patron para dividir donde no encuentre un caracter alfanumerico
def compil(texto):
    #tex=[]
    a=re.compile(r'\W+')
    tex=a.split(texto)
    
    return tex


# process text 
def preprocesar_df(df, col_name, clean_col_name):
    df[clean_col_name] = df[col_name].apply(lambda x: minuscula (x))\
                                    .apply(lambda x: remover_url(x))\
                                    .apply(lambda x: remover_punto(x))\
                                    .apply(lambda x: remover_usuario(x))\
                                    .apply(lambda x: remover_tag_html(x))\
                                    .apply(lambda x: remover_acento(x))\
                                    .apply(lambda x: remover_numero(x))\
                                    .apply(lambda x: remover_no_alfabeto(x))\
                                    .apply(lambda x: remover_espacio_extra(x))\
                                    .apply(lambda x: remove_stopwords(x))\
                                    .apply(lambda x: compil(x))
                                            











#remueve numeros 
def remove_numbers(sentence):
    cleaned_sentence = re.sub(r'\d+', '', sentence)
    return cleaned_sentence
# remueve la palabra speaker decada oración 
def remove_spek(sentence):
    cleaned_sentence = re.sub(r'SPEAKER_', '', sentence)
    return cleaned_sentence

def clean_data_tex(data, column_tex, value_to_find):
   
    df = data[data[column_tex].str.contains(value_to_find)]
    df.rename(columns={column_tex: 'MensajeRespuesta'}, inplace=True)
    df['MensajeRespuesta'] = df['MensajeRespuesta'].astype(str)
    
    return df
def genera_token1(df, column):
    #df[column] = df[column].apply(remove_numbers)
    df.rename(columns={column:'MensajeRespuesta2'}, inplace=True)

    df['MensajeRespuesta2'] = df['MensajeRespuesta2'].astype(str)
    preprocesar_df(df, 'MensajeRespuesta2', 'Mensajes')                                    #.apply(lambda x: remover_stop_word2(x))
    dff=df.explode(column='Mensajes')

    dff.rename(columns={'Mensajes': 'token'}, inplace=True)
    return dff
def clean_data_tex1(df, column_tex):
    df.rename(columns={column_tex: 'MensajeRespuesta'}, inplace=True)
    #df['MensajeRespuesta'] = df['MensajeRespuesta'].astype(str)
    df1=df.copy()
    return df1




def genera_token(df, column):
    df[column] = df[column].apply(remove_numbers)
    df['MensajeRespuesta2'] = df[column].apply(remove_spek)
    preprocesar_df(df, 'MensajeRespuesta2', 'Mensajes')                                    #.apply(lambda x: remover_stop_word2(x))
    dff=df.explode(column='Mensajes')

    dff.rename(columns={'Mensajes': 'token'}, inplace=True)
    return dff

import numpy as np
import pandas as pd
import datetime as dt
from funciones_limpieza import *
from lectura import *



def normalizar_emisiones(df):
    df.replace({'US':'UNITED STATES OF AMERICA (THE)',
         'TRINIDAD & TOBAGO' :'TRINIDAD AND TOBAGO',
          'VENEZUELA' :'VENEZUELA (BOLIVARIAN REPUBLIC OF)',
          'CZECH REPUBLIC':'CZECHIA',
          'NETHERLANDS' :'NETHERLANDS (THE)',
          'NORTH MACEDONIA' :'REPUBLIC OF NORTH MACEDONIA',
          'UNITED KINGDOM':'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND (THE)',
          'RUSSIAN FEDERATION':'RUSSIAN FEDERATION (THE)',
          'IRAN':'IRAN (ISLAMIC REPUBLIC OF)',
          'UNITED ARAB EMIRATES': 'UNITED ARAB EMIRATES (THE)',
          'MIDDLE AFRICA':'CENTRAL AFRICAN REPUBLIC (THE)',
          'CHINA HONG KONG SAR':'HONG KONG',
          'PHILIPPINES':'PHILIPPINES (THE)',
          'SOUTH KOREA':'KOREA (THE REPUBLIC OF)',
          'TAIWAN':'TAIWAN (PROVINCE OF CHINA)',
          'VIETNAM':'VIET NAM'
          },inplace=True)#de los 82 paises registados se eliminan 11 registros porque no estan asignados a un pais especifico o es una clasificacion no bien definida
    return df

def formato_emisiones(df):
       df.rename(columns={'Pais':'country_name'},inplace=True)
       
       df['country_name']=df['country_name'].str.upper()
      
       df.drop([0], axis=0, inplace=True)
       df.drop(["2021.1", "2011-21", "2021.2"], axis=1, inplace=True)
       df=normalizar_emisiones(df)
       #print(df['country_name'].unique())
       lista=comparar_nombre_pais(df)
       df.reset_index(inplace=True)
       falsos=no_paises_filas(lista)
       df.drop(['index'], axis=1,inplace=True)
       #print(f'falsos: {falsos}')
       df=elimimar_filas_incorrectas(falsos, df)
    
       return df

def limpieza_cap_instalada(df):
    df=trabajar_nulos_ceros_otros(df)
    df.rename(columns={'Pais':'country_name'},inplace=True)
    df.drop(columns=['2021.1','2011-2021','2021.2','1995','1996'],axis=1,inplace=True)
    df['country_name']=df['country_name'].str.upper()
    #df.rename(columns={'Country Name':'country_name','Country Code':'country_code'},inplace=True)
    #print(df)
    #print(df['country_name'].unique())
    df=normalizar_emisiones(df)
    lista=comparar_nombre_pais(df)
    df.reset_index(inplace=True)
    falsos=no_paises_filas(lista)
    df.drop(['index'], axis=1,inplace=True)
    #print(f'falsos: {falsos}')
    df=elimimar_filas_incorrectas(falsos, df)
    
    return df
    
           
#funcion para agregar id tipo  de energia
def agregar_column_t_energia(df,tipo_ener):
    tipo_ener.upper()
    if tipo_ener=='SOLAR':
       df.insert(2, 'id_energy', 1)
    elif tipo_ener=='VIENTO' :
       df.insert(2, 'id_energy', 2) 
    elif tipo_ener=='CARBON':
        df.insert(2, 'id_energy', 3)
    elif tipo_ener=='PETROLEO':
        df.insert(2, 'id_energy', 4)
    elif tipo_ener=='NUCLEAR':
        df.insert(2, 'id_energy', 5)
    elif tipo_ener=='GAS NATURAL':
        df.insert(2, 'id_energy', 6)
    elif tipo_ener=='GEOTERMICA':    
        df.insert(2, 'id_energy', 7)
    elif tipo_ener=='HIDROELECTRICA':
        df.insert(2, 'id_energy', 8)
    else:
        df
    return df
"""
for i,p in enumerate(df_años['year']):
        cod= df_años.loc[i,'id_year']
        df.loc[df['year']==p,'id_year']=cod

"""
def calculos_petroleo(df):
    #print('en funcion petrole')
    #df=df.astype({'annual_production':'int64'})
    #p=10**(-9)
    #eq_xj=6.11786319999928*p
    twh=588.4407483972509
    for i, ele in enumerate(df['annual_production']): 
        totaltwh=df.loc[i,'annual_production']*twh
        df.loc[i,'annual_production']=totaltwh
    return(df)

def conversion_exaj_twh(df):
    #print('desde conversion')
    #print(df.dtypes)
    twh=227.77778 #equivalencia en twh de un joule
    #print(twh)
    for i, ele in enumerate(df['annual_production']):
        s=df.loc[i,'annual_production']
        #print(f's:{s}')
        prod_twh=df.loc[i,'annual_production']*twh
        #print(prod_twh)
        df.loc[i,'annual_production']=prod_twh
        

    return(df)    

    # 8765 MBOE / 588.4407483972509
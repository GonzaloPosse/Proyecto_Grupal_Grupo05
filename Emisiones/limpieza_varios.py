# extraemos las url desde git en una lista
# en este scrip encontramos los df paises,irradianza, poblacion,años,tipo_energia,
# intesidad_carbono, consumo_energia


import pandas as pd
from lectura import *
from funciones_limpieza import *
from funciones_limpieza2 import*
import numpy as np
from sqlalchemy import create_engine


print('*////////DF_PAISES/////*')
#funcion para obtener la info de irradianza desde el datalake
    #url para obtener los paises y sus codigos
url = 'https://www.iban.com/country-codes'
paises=limpiando_pais(url)
print(paises)
print(paises.dtypes)
print('*********************FIN PAISES*************************')



print('***********DF_AÑOS**********************')
años=tabla_años()
print(años)
print(años.dtypes)
print('*********************FIN AÑOS*************************')


#funcion para obtener la info de irradianza desde el datalake
urls=[
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202010.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202011.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202012.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202013.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202014.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202015.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202016.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202017.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202018.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202019.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202020.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202021.csv',
        'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Global%20horizontal%20irradiance%202022.csv'
    ]

print('***********IRRADIANZA**********************')
df_irra=lectura_irra(urls)
    
df_irra_final=limpieza_irradianza(df_irra)
df_irra_final=agregar_id_pais(paises,df_irra_final)
    #este paso se realzia porque los nombres de las columnas son str e int
df_irra_final=mapeo_colum_años(df_irra_final)
df_irra_final=funcion_reshape(df_irra_final)
df_irra_final=agregar_cod_año(años,df_irra_final)
df_irra_final=agregar_id_propio_tabla(df_irra_final,'id_irradiation')
    #print('******** DF_IRRADIANZA FINAL****************')
print(df_irra_final)
print(df_irra_final.dtypes)
print('*********************FIN IRRADIANZA*************************')

    


#funcion para obtener la info de poblacion desde el datalake
    #print('*****//////DF_POBLACION*****///////')
    #obtenemos la poblacion por pais por año
df_poblacion=lectura_archivos('https://raw.githubusercontent.com//MAKACT//proyectoConsumoEnergiaEquipo5//main//Poblacion.csv')#pasar el archivo al git
df_poblacion=limpieza_poblacion(df_poblacion)
df_poblacion=agregar_id_pais_poblacion(paises,df_poblacion)
    #print(None in df_poblacion)
df_poblacion=reshape_poblacion(df_poblacion,'annual_population')
    #df_poblacion['annual_population']=df_poblacion['annual_population'].astype(int)
df_poblacion=agregar_cod_año(años,df_poblacion)
df_poblacion=agregar_id_propio_tabla(df_poblacion,'id_population')
print(df_poblacion)
print(df_poblacion.dtypes)
print('*********************FIN POBLACION*************************')


print('***********DF_CATALOGO_ENERGIA//////////')
df_energia=catalogo_energia()
print(df_energia)
print(df_energia.dtypes)
print('*********************FIN CATALOGO ENERGIA*************************')

#funcion para obtener la info de emisiones desde el datalake

print('*****/// DF_EMISIONES////*******')
url_e='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Emisiones%20de%20dioxido%20de%20carbono%20totales.csv'
df_emisiones= lectura_archivos(url_e)
df_emisiones=trabajar_nulos_ceros_otros(df_emisiones)
df_emisiones=formato_emisiones(df_emisiones)
df_emisiones=agregar_id_pais(paises,df_emisiones)
df_emisiones=reshape_poblacion(df_emisiones,'annual_emission_co2')
df_emisiones=agregar_cod_año(años,df_emisiones)
df_emisiones=agregar_id_propio_tabla(df_emisiones,'id_emission')
print(df_emisiones)
print(df_emisiones.dtypes)
print('*********************FIN EMISIONES*************************')

print('////****COSUMO////*****')
def formato_consumo(url,tipo_energia):
        df_consumo=lectura_archivos(url)
        trabajar_nulos_ceros_otros(df_consumo)
        #print(df_consumo.isna().sum())
        formato_emisiones(df_consumo)
        agregar_id_pais(paises,df_consumo)
        df_consumo=reshape_poblacion(df_consumo,'annual_consumption')
        df_consumo=df_consumo.astype({'annual_consumption':'float'})
        df_consumo=agregar_cod_año(años,df_consumo)
        df_consumo=agregar_column_t_energia(df_consumo,tipo_energia)
        #df_con_bio=agregar_id_propio_tabla(df_con_bio,'id_consumption')
        return df_consumo

    #print('******CARBON*****')
    #ESTA ENERGIA ESTA EN EXAJOULES
url_con= 'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Carbon%20Exajoules.csv'
df_con_car=formato_consumo(url_con,'CARBON')
    #print(df_con_car)

    #print('///****************BIOTERMICA*//////')
    #ESTA ENERGIA ESTA EN EXAJOULES
url_b='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Energia%20Botermica%20Biomasa%20otras%20Exajoules.csv'
df_con_bio=formato_consumo(url_b,'GEOTERMICA')
    #print(df_con_bio)


    #print('///****************EOLICA*//////')
url_eol='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Energia%20Eolica%20Exajoules.csv'
df_con_eol=formato_consumo(url_eol,'VIENTO')
    #print(df_con_eol)

    #print('///****************NUCLEAR*//////')
url_nuc= 'https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Energia%20Nuclear%20Exajoules.csv'
df_con_nuc=formato_consumo(url_nuc,'NUCLEAR')
    #print(df_con_nuc)

    #print('///****************SOLAR*//////')
url_sol='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Energia%20Solar%20Exajoules.csv'
df_con_sol=formato_consumo(url_sol,'SOLAR')
    #print(df_con_sol)


    #print('///****************GAS*//////')
url_gas='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Gas%20Natural%20Exajoules.csv'
df_con_gas=formato_consumo(url_gas,'GAS NATURAL')
    #print(df_con_gas)
    #unimos todos los df_consumo
    #print(df_con_gas)

    #print('///****************HIDRO*//////')
url_hid='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Hidroelectricidad%20Exajoules.csv'
df_con_hid=formato_consumo(url_hid,'HIDROELECTRICA')
    #print(df_con_hid)


    #print('///****************PETROLEO*//////')
url_pet='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Consumo%20Petroleo%20Exajoules.csv'
df_con_pet=formato_consumo(url_pet,'HIDROELECTRICA')
    #print(df_con_pet)


    #Se arma el dataframe de consumo
    #funcion para obtener la info de consumo desde el datalake

df_consumo_all=pd.concat([df_con_car,df_con_car,df_con_eol,df_con_nuc,df_con_sol,df_con_gas,df_con_hid,df_con_pet],axis=0)

df_consumo_all=agregar_id_propio_tabla(df_consumo_all,'id_consumption')
print(df_consumo_all)
print(df_consumo_all.dtypes)
print('*********************FIN CONSUMO*************************')
#confirmamos valores iguiles a 0 en la tabla 
#print(0 in df_consumo_all)
#print(df_consumo_all.dtypes)

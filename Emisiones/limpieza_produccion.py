import pandas as pd
from lectura import *
from funciones_limpieza import *
from funciones_limpieza2 import *
from limpieza_varios import*

import numpy as np

    #funcion para limpiar  cvs produccion
def formato_completo_prod(url,tipo_energia):
        df_prod=lectura_archivos(url)    
        df_prod=trabajar_nulos_ceros_otros(df_prod)
        df_prod=formato_emisiones(df_prod)
        agregar_id_pais(paises,df_prod)
        df_prod=reshape_poblacion(df_prod,'annual_production')
        indices=df_prod[df_prod['annual_production']== 0.0].index
        df_prod.drop(indices,inplace=True)
        df_prod=df_prod.astype({'annual_production':'float'})
        df_prod=agregar_cod_año(años,df_prod)
        df_prod=agregar_column_t_energia(df_prod,tipo_energia)
        return df_prod


def formato_completo_prod_gas_carb(url,tipo_energia):
        df_prod_exa_to_twh=lectura_archivos(url)     
        df_prod_exa_to_twh=trabajar_nulos_ceros_otros(df_prod_exa_to_twh)
        df_prod_exa_to_twh=formato_emisiones(df_prod_exa_to_twh)
        agregar_id_pais(paises,df_prod_exa_to_twh)
        df_prod_exa_to_twh=reshape_poblacion(df_prod_exa_to_twh,'annual_production')
        df_prod_exa_to_twh=df_prod_exa_to_twh.astype({'annual_production':'float'})
        indices0=df_prod_exa_to_twh[df_prod_exa_to_twh['annual_production']== 0].index
        df_prod_exa_to_twh.drop(indices0,inplace=True)
        indices=df_prod_exa_to_twh[df_prod_exa_to_twh['annual_production']== 0.0].index
        df_prod_exa_to_twh.drop(indices,inplace=True)
        df_prod_exa_to_twh.reset_index(inplace=True)
        df_prod_exa_to_twh=conversion_exaj_twh(df_prod_exa_to_twh)
        df_prod_exa_to_twh=agregar_cod_año(años,df_prod_exa_to_twh)
        df_prod_exa_to_twh=agregar_column_t_energia(df_prod_exa_to_twh,tipo_energia)
        df_prod_exa_to_twh.round({'annual_production':2})
        return df_prod_exa_to_twh    

def formato_completo_pretroleo(url,tipo_energia):
        df_prod_petr=lectura_archivos(url)       
        df_prod_petr=trabajar_nulos_ceros_otros(df_prod_petr)
        df_prod_petr=formato_emisiones(df_prod_petr)
        agregar_id_pais(paises,df_prod_petr)
        df_prod_petr=reshape_poblacion(df_prod_petr,'annual_production')
        df_prod_petr=df_prod_petr.astype({'annual_production':'float'})
        indices0=df_prod_petr[df_prod_petr['annual_production']== 0].index
        df_prod_petr.drop(indices0,inplace=True)
        indices=df_prod_petr[df_prod_petr['annual_production']== 0.0].index
        df_prod_petr.drop(indices,inplace=True)
        df_prod_petr.reset_index(inplace=True)
        df_prod_petr=calculos_petroleo(df_prod_petr)
        df_prod_petr=agregar_cod_año(años,df_prod_petr)
        df_prod_petr=agregar_column_t_energia(df_prod_petr,tipo_energia)
        return df_prod_petr    

    #limpiando csv prod eolica
    #print('*****************PROD EOLICA******************')    
url_e='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Energia%20Eolica%20twh.csv'
prod_eol=formato_completo_prod(url_e,'VIENTO')
    #print('*****************PROD GEOTERMICA******************')
url_ge='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Energia%20Geotermica%20Biomasa%20y%20otras%20twh.csv'
prod_ge=formato_completo_prod(url_ge,'GEOTERMICA')
    #print('*****************PROD NUCLEAR******************')
url_n='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Energia%20Nuclear%20twh.csv'
prod_nu=formato_completo_prod(url_n,'NUCLEAR')
    #print('*****************PROD SOLAR******************')
url_s='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Energia%20Solar.csv'
prod_sol=formato_completo_prod(url_s,'SOLAR')
    #print('*****************PROD HIDRO******************')
url_h='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Hidroelectricidad%20twh.csv'
prod_h=formato_completo_prod(url_h,'HIDROELECTRICA')
    #print('*****************PROD GAS NATURAL******************')
url_g='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Produccion%20Gas%20Natural%20Exajoules.csv'
prod_g=formato_completo_prod_gas_carb(url_g,'GAS NATURAL')

    #print('*****************PROD CARBON******************')
url_c='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Produccion%20de%20Carbono%20Exajoules.csv'
prod_c=formato_completo_prod_gas_carb(url_c,'CARBON')

    #print('*****************PROD PETROLEO******************')
url_p='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Produccion%20Petroleo%20Barriles%20Milles%20Diarios.csv'
prod_p=formato_completo_pretroleo(url_p,'PETROLEO')


    #Se arma el dataframe de consumo

df_produccion_all=pd.concat([prod_eol,prod_ge,prod_nu,prod_sol,prod_h,prod_g,prod_c,prod_p],axis=0)

df_produccion_all=agregar_id_propio_tabla(df_produccion_all,'id_production')
    #confirmamos valores iguiles a 0 en la tabla 


df_produccion_all.drop(['index'], axis=1,inplace=True)
print('*****************PRODUCCION******************')
print(df_produccion_all)
print(df_produccion_all.dtypes)
print('*********************FIN PRODUCCION*************************')
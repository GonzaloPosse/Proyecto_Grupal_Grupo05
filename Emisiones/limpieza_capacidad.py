import pandas as pd
from lectura import *
from funciones_limpieza import *
from funciones_limpieza2 import *
from limpieza_varios import*
import numpy as np

from funciones_limpieza2 import limpieza_cap_instalada

    #url_aero='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Capacida%20Instalada%20Aerogeneradores%20Megawatts.csv'

    #url_foto='https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Energia%20Eolica%20twh.csv'
    #df_inst_aero=lectura_archivos(url_aero)
    #limpieza de capacidad instalada de aerogeneradores
df_inst_aero=pd.read_csv('https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Capacida%20Instalada%20Aerogeneradores%20Megawatts.csv',on_bad_lines='skip')
    #print(df_inst_aero.info())
df_inst_aero=limpieza_cap_instalada(df_inst_aero)
agregar_id_pais(paises,df_inst_aero)
df_inst_aero=reshape_poblacion(df_inst_aero,'installed_capacity')
df_inst_aero=agregar_cod_año(años,df_inst_aero)
df_inst_aero=agregar_column_t_energia(df_inst_aero,'VIENTO')
    #print(df_inst_aero)

    #limpieza de capacidad instalada de fotovoltaicas
df_inst_foto=pd.read_csv('https://raw.githubusercontent.com/MAKACT/proyectoConsumoEnergiaEquipo5/main/Generacion%20Energia%20Eolica%20twh.csv',on_bad_lines='skip')
    #print(df_inst_foto.info())
df_inst_foto=trabajar_nulos_ceros_otros(df_inst_foto)
df_inst_foto=formato_emisiones(df_inst_foto)
agregar_id_pais(paises,df_inst_foto)
df_inst_foto=reshape_poblacion(df_inst_foto,'installed_capacity')
df_inst_foto=agregar_cod_año(años,df_inst_foto)
df_inst_foto=agregar_column_t_energia(df_inst_foto,'SOLAR')
    #print(df_inst_foto)

print('*************CAPACIDAD INSTALADA TOTAL****************')

df_capacidad_instalda_all=pd.concat([df_inst_aero,df_inst_foto],axis=0)
df_capacidad_instalda_all=agregar_id_propio_tabla(df_capacidad_instalda_all,'id_installed_capacity')
        #print(df_capacidad_instalda_all)
        #confirmamos valores iguiles a 0 en la tabla 
        #print(0 in df_capacidad_instalda_all)
print(df_capacidad_instalda_all)
print(df_capacidad_instalda_all.dtypes)
print('*********************FIN CAPACIDAD INSTALDA*************************')

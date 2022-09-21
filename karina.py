from asyncore import write
from optparse import Option
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from time import sleep
from geopy.geocoders import Nominatim
import pandas as pd
import geopandas
import folium
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

conexion = create_engine("postgresql://{user}:{pw}@{host}:{port}/{db}"
                         .format(user="postgres",
                                 pw="postgres",
                                 host="database-grupo5.cgmzd7suyc4v.us-east-1.rds.amazonaws.com",
                                 port="5432",
                                 db="postgres",
                                 echo=False))
if conexion.connect():
  print("conexion exitosa")

#*****EMISIONES CO2 DEL 90 AL 2021
emisionesk = pd.read_sql_query('SELECT *FROM "Emisiones_CO2";', con=conexion)
paisesk = pd.read_sql_query('SELECT *FROM "Paises";', con=conexion)
añosk = pd.read_sql_query('SELECT *FROM "Años";', con=conexion)
emisiones80k=pd.merge(emisionesk,paisesk,on='Id_Pais')
emisiones80k=pd.merge(emisiones80k,añosk,on='Id_anio')
emisiones80k.drop(columns={'Id_Pais','Id_anio'}, inplace=True)
emisiones_añok=emisiones80k.groupby(by=['Anio']).mean()
emisiones_añok['Variacion']=(emisiones_añok['Emisiones_CO2'].pct_change())*100
fig = go.Figure()
fig.add_trace(go.Scatter(x=emisiones_añok.index, y=emisiones_añok['Variacion'],
                    mode='lines'
                     ))
fig.update_layout(title='Porcentaje de incremento de Emisiones CO2 en el mundo por año de  1990 a 2021',
                   xaxis_title='Año',
                   yaxis_title='Porcentaje de incremento')
st.plotly_chart(fig, use_container_width=True)  

#***************CONSUMO ENERGIAS RENOVABLES***********

carbonK=pd.read_sql_query('SELECT *FROM "Energia_Carbon";', con=conexion)
carbonK.rename(columns={'Id_Anio':'Id_anio'},inplace=True)
petroleoK=pd.read_sql_query('SELECT *FROM "Energia_petroleo";', con=conexion)
gasK=pd.read_sql_query('SELECT *FROM "Gas_Natural";', con=conexion)
ene_fosilK=pd.merge(carbonK,petroleoK, on='Id_anio')
ene_fosilK=pd.merge(ene_fosilK,gasK, on='Id_anio')
ene_fosilK.drop(columns={'Id_Pais_x','ID_Energia_petroleo','Id_Pais_y','ID_Gas_Natural'},inplace=True)
ene_fosil_consumoK=ene_fosilK.loc[:,('Id_anio','Id_Pais','Consumo_Carbon','Consumo_Petroleo','Consumo_gas')]
ene_fosil_consumoK['Total_Consumo']=ene_fosil_consumoK['Consumo_Carbon']+ene_fosil_consumoK['Consumo_Petroleo']+ene_fosil_consumoK['Consumo_gas']
ene_fosil_consumo_totalK=pd.merge(ene_fosil_consumoK,paisesk, on='Id_Pais')
ene_fosil_consumo_totalK=pd.merge(ene_fosil_consumo_totalK,añosk, on='Id_anio')
indexN=ene_fosil_consumo_totalK[ene_fosil_consumo_totalK['Pais']=='Other Europe (BP)'].index
ene_fosil_consumo_totalK.drop(indexN, inplace=True)
indexN=ene_fosil_consumo_totalK[ene_fosil_consumo_totalK['Pais']=='Other CIS (BP)'].index
ene_fosil_consumo_totalK.drop(indexN, inplace=True)
indexN=ene_fosil_consumo_totalK[ene_fosil_consumo_totalK['Pais']=='Other Middle East (BP)'].index
ene_fosil_consumo_totalK.drop(indexN, inplace=True)
indexN=ene_fosil_consumo_totalK[ene_fosil_consumo_totalK['Pais']=='Other Western Africa (BP)'].index
ene_fosil_consumo_totalK.drop(indexN, inplace=True)
indexN=ene_fosil_consumo_totalK[ene_fosil_consumo_totalK['Pais']=='Other Northern Africa (BP)'].index
ene_fosil_consumo_totalK.drop(indexN, inplace=True)
indexN=ene_fosil_consumo_totalK[ene_fosil_consumo_totalK['Pais']=='Other Asia Pacific (BP)'].index
ene_fosil_consumo_totalK.drop(indexN, inplace=True)
ene_fosil_consumo_total_añoK=ene_fosil_consumo_totalK.groupby(by=['Anio']).mean()
ene_fosil_consumo_total_paisK=ene_fosil_consumo_totalK.groupby(by=['Pais']).mean()

nucleark=pd.read_sql_query('SELECT *FROM "Energia_nuclear_con_ceros";', con=conexion)
ene_renK=pd.read_sql_query('SELECT *FROM "Energia_renovables";', con=conexion)
todas_renK=pd.merge(ene_renK,paisesk, on='Id_Pais')
todas_renK=pd.merge(todas_renK,añosk, on='Id_anio')
todas_renK=pd.merge(todas_renK,nucleark, on='Id_anio')
todas_renK.drop(columns={'ID_Energia_renovables','Id_Pais_x','Id_anio','Id_Pais_y','ID_Energia_nuclear'},inplace=True)
indexN=todas_renK[todas_renK['Pais']=='Other Europe (BP)'].index
todas_renK.drop(indexN, inplace=True)
indexN=todas_renK[todas_renK['Pais']=='Other CIS (BP)'].index
todas_renK.drop(indexN, inplace=True)
indexN=todas_renK[todas_renK['Pais']=='Other Middle East (BP)'].index
todas_renK.drop(indexN, inplace=True)
indexN=todas_renK[todas_renK['Pais']=='Other Western Africa (BP)'].index
todas_renK.drop(indexN, inplace=True)
indexN=todas_renK[todas_renK['Pais']=='Other Northern Africa (BP)'].index
todas_renK.drop(indexN, inplace=True)
indexN=todas_renK[todas_renK['Pais']=='Other Asia Pacific (BP)'].index
todas_renK.drop(indexN, inplace=True)
todas_lim_paisK=todas_renK.groupby(by=['Pais']).mean()
todas_lim_paisK['total_consumo'] = (todas_lim_paisK['Consumo_renovables']+todas_lim_paisK['Consumo_Nuclear'])/2
todas_lim_paisK['total_produccion'] = (todas_lim_paisK['Produccion_renovables']+todas_lim_paisK['Produccion_Nuclear'])/2
todas_lim_añoK=todas_renK.groupby(by=['Anio']).mean()
todas_lim_añoK['total_consumo'] = (todas_lim_añoK['Consumo_renovables']+todas_lim_añoK['Consumo_Nuclear'])/2
todas_lim_añoK['total_producion'] = (todas_lim_añoK['Produccion_renovables']+todas_lim_añoK['Produccion_Nuclear'])/2
con_añok= go.Figure()
con_añok.add_trace(go.Scatter(x=todas_lim_añoK.index, y=todas_lim_añoK['total_consumo'],
                    mode='lines',
                    name='Energia Limpia' ))
con_añok.add_trace(go.Scatter(x=ene_fosil_consumo_total_añoK.index, y=ene_fosil_consumo_total_añoK['Total_Consumo'],
                    mode='lines',
                    name='Energia fosil'))
con_añok.update_layout(title='Consumo Energia fosil vs Energia limpia',
                   xaxis_title='Año',
                   yaxis_title='Consumo(Exajoules)')
st.plotly_chart(con_añok, use_container_width=True)
#fig.show()
fig = go.Figure()
fig.add_trace(go.Scatter(x=todas_lim_paisK.index, y=todas_lim_paisK['total_consumo'],
                    mode='lines',
                    name='Energía Limpia' ))
fig.add_trace(go.Scatter(x=ene_fosil_consumo_total_paisK.index, y=ene_fosil_consumo_total_paisK['Total_Consumo'],
                    mode='lines',
                    name='Energía fosil'))
fig.update_layout(title='Consumo Energía fosil vs Energía limpia por país',
                   xaxis_title='Año',
                   yaxis_title='Consumo(Exajoules)')
st.plotly_chart(fig, use_container_width=True)
#fig.show()

##energia limpia******
def lectura_consumo_limpio(pais):
    eoil=pd.read_sql_query('SELECT *FROM "Energia_Eolica";', con=conexion)
    solar=pd.read_sql_query('SELECT *FROM "Energia_solar";', con=conexion)
    hidro=pd.read_sql_query('SELECT *FROM "Energia_Hidroelectrica";', con=conexion)
    hidro.rename(columns={'Id_Anio':'Id_anio'}, inplace=True)
    geo=pd.read_sql_query('SELECT *FROM "Energia_Geotermica";', con=conexion)
    geo.rename(columns={'Id_Anio':'Id_anio'}, inplace=True)
    nuclear=pd.read_sql_query('SELECT *FROM "Energia_nuclear_con_ceros";', con=conexion)
    todas=pd.merge(solar,eoil, on='Id_anio')
    todas=pd.merge(todas,hidro, on='Id_anio')
    todas.drop(columns={'Id_Pais_x','Id_Pais_y','Id_Energia_Eolica','Id_Energia_Hidroelectrica'},inplace=True)
    todas=pd.merge(todas, añosk, on='Id_anio')
    todas90=todas[todas['Anio']>=1990]
    todas90=pd.merge(todas90,paisesk, on='Id_Pais')
    indexN=todas90[todas90['Pais']=='Other Europe (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other CIS (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Middle East (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Western Africa (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Northern Africa (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Asia Pacific (BP)'].index
    todas90.drop(indexN, inplace=True)
    todas90.drop(columns={'Id_Energia_solar','Id_anio','Id_Pais'},inplace=True)
    todas90['Consumo_Eolica']=todas90['Consumo_Eolica'].astype(float)
    todas90['Produccion_Eolica']=todas90['Produccion_Eolica'].astype(float)
    todas90['Capacidad_instalada_Eolica']=todas90['Capacidad_instalada_Eolica'].astype(float)
    todas90['Consumo_Hidroelectrica']=todas90['Consumo_Hidroelectrica'].astype(float)
    pais_lim1=todas90[(todas90.Pais==pais)].groupby(by=['Anio']).mean()
    geo_nuc=pd.merge(geo, paisesk, on='Id_Pais')
    geo_nuc=pd.merge(geo_nuc, añosk, on='Id_anio')
    geo_nuc=pd.merge(geo_nuc,nuclear, on='Id_anio')
    geo_nuc.drop(columns={'Id_Pais_x','Id_Pais_y','Id_anio','Id_Energia_Geotermica','ID_Energia_nuclear'},inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Europe (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other CIS (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Middle East (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Western Africa (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Northern Africa (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Asia Pacific (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    geo_nuc['Consumo_Geotermica']=geo_nuc['Consumo_Geotermica'].astype(float)
    pais_lim2=geo_nuc[(geo_nuc.Pais==pais)].groupby(by=['Anio']).mean()
    return pais_lim1,pais_lim2
#***********Energia fosil********************

def grafica_consumo_fosil(pais):
    carbon=pd.read_sql_query('SELECT *FROM "Energia_Carbon";', con=conexion)
    carbon.rename(columns={'Id_Anio':'Id_anio'},inplace=True)
    petroleo=pd.read_sql_query('SELECT *FROM "Energia_petroleo";', con=conexion)
    gas=pd.read_sql_query('SELECT *FROM "Gas_Natural2";', con=conexion)
    ene_fosil=pd.merge(carbon,petroleo, on='Id_anio')
    ene_fosil=pd.merge(ene_fosil,gas,how='left', on='Id_anio')
    ene_fosil.drop(columns={'Id_Pais_x','ID_Energia_petroleo','Id_Pais_y','Id_Energia_Gas'},inplace=True)
    ene_fosil_consumo=ene_fosil.loc[:,('Id_anio','Id_Pais','Consumo_Carbon','Consumo_Petroleo','Consumo_Gas_Natural')]
    ene_fosil_consumo.fillna(0, inplace=True)
    ene_fosil_consumo['Total_Consumo']=ene_fosil_consumo['Consumo_Carbon']+ene_fosil_consumo['Consumo_Petroleo']+ene_fosil_consumo['Consumo_Gas_Natural']
    ene_fosil_consumo_total=pd.merge(ene_fosil_consumo,paisesk, on='Id_Pais')
    ene_fosil_consumo_total=pd.merge(ene_fosil_consumo_total,añosk, on='Id_anio')
    indexN=ene_fosil_consumo_total[ene_fosil_consumo_total['Pais']=='Other Europe (BP)'].index
    ene_fosil_consumo_total.drop(indexN, inplace=True)
    indexN=ene_fosil_consumo_total[ene_fosil_consumo_total['Pais']=='Other CIS (BP)'].index
    ene_fosil_consumo_total.drop(indexN, inplace=True)
    indexN=ene_fosil_consumo_total[ene_fosil_consumo_total['Pais']=='Other Middle East (BP)'].index
    ene_fosil_consumo_total.drop(indexN, inplace=True)
    indexN=ene_fosil_consumo_total[ene_fosil_consumo_total['Pais']=='Other Western Africa (BP)'].index
    ene_fosil_consumo_total.drop(indexN, inplace=True)
    indexN=ene_fosil_consumo_total[ene_fosil_consumo_total['Pais']=='Other Northern Africa (BP)'].index
    ene_fosil_consumo_total.drop(indexN, inplace=True)
    indexN=ene_fosil_consumo_total[ene_fosil_consumo_total['Pais']=='Other Asia Pacific (BP)'].index
    ene_fosil_consumo_total.drop(indexN, inplace=True)
    año90=ene_fosil_consumo_total[ene_fosil_consumo_total.Anio>=1990]
    print(año90)
    print(año90[año90.Pais=='China'])
    pais=año90[(año90.Pais==pais)].groupby(by=['Anio']).mean()

    
    fig = go.Figure()
    fig.add_trace(go.Line(
    x=pais.index,
    y=pais['Consumo_Carbon'],
    name='Carbon',
    marker_color= 'lightslategray'
        ))
    fig.add_trace(go.Line(
    x=pais.index,
    y=pais['Consumo_Petroleo'],
    name='Petroleo',
    marker_color='black'
        ))
    fig.add_trace(go.Line(
    x=pais.index,
    y=pais['Consumo_Gas_Natural'],
    name='Gas Natural',
    marker_color='yellowgreen'
        ))

    

    fig.update_layout(title='Consumo Energia fosil',
                   xaxis_title='Año',
                   yaxis_title='Consumo(Exajoules)')
    
    return st.plotly_chart(fig, use_container_width=True)





#FUNCION PARA GRAFICAR ENERGIAS LIMPIAS
def grafica_consumo_limpio(pais):
    eoil=pd.read_sql_query('SELECT *FROM "Energia_Eolica";', con=conexion)
    solar=pd.read_sql_query('SELECT *FROM "Energia_solar";', con=conexion)
    hidro=pd.read_sql_query('SELECT *FROM "Energia_Hidroelectrica";', con=conexion)
    hidro.rename(columns={'Id_Anio':'Id_anio'}, inplace=True)
    geo=pd.read_sql_query('SELECT *FROM "Energia_Geotermica";', con=conexion)
    geo.rename(columns={'Id_Anio':'Id_anio'}, inplace=True)
    nuclear=pd.read_sql_query('SELECT *FROM "Energia_nuclear_con_ceros";', con=conexion)
    todas=pd.merge(solar,eoil, on='Id_anio')
    todas=pd.merge(todas,hidro, on='Id_anio')
    todas.drop(columns={'Id_Pais_x','Id_Pais_y','Id_Energia_Eolica','Id_Energia_Hidroelectrica'},inplace=True)
    todas=pd.merge(todas, añosk, on='Id_anio')
    todas90=todas[todas['Anio']>=1990]
    todas90=pd.merge(todas90,paisesk, on='Id_Pais')
    indexN=todas90[todas90['Pais']=='Other Europe (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other CIS (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Middle East (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Western Africa (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Northern Africa (BP)'].index
    todas90.drop(indexN, inplace=True)
    indexN=todas90[todas90['Pais']=='Other Asia Pacific (BP)'].index
    todas90.drop(indexN, inplace=True)
    todas90.drop(columns={'Id_Energia_solar','Id_anio','Id_Pais'},inplace=True)
    todas90['Consumo_Eolica']=todas90['Consumo_Eolica'].astype(float)
    todas90['Produccion_Eolica']=todas90['Produccion_Eolica'].astype(float)
    todas90['Capacidad_instalada_Eolica']=todas90['Capacidad_instalada_Eolica'].astype(float)
    todas90['Consumo_Hidroelectrica']=todas90['Consumo_Hidroelectrica'].astype(float)
    pais_lim1=todas90[(todas90.Pais==pais)].groupby(by=['Anio']).mean()
    geo_nuc=pd.merge(geo, paisesk, on='Id_Pais')
    geo_nuc=pd.merge(geo_nuc, añosk, on='Id_anio')
    geo_nuc=pd.merge(geo_nuc,nuclear, on='Id_anio')
    geo_nuc.drop(columns={'Id_Pais_x','Id_Pais_y','Id_anio','Id_Energia_Geotermica','ID_Energia_nuclear'},inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Europe (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other CIS (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Middle East (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Western Africa (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Northern Africa (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    indexN=geo_nuc[geo_nuc['Pais']=='Other Asia Pacific (BP)'].index
    geo_nuc.drop(indexN, inplace=True)
    geo_nuc['Consumo_Geotermica']=geo_nuc['Consumo_Geotermica'].astype(float)
    pais_lim2=geo_nuc[(geo_nuc.Pais==pais)].groupby(by=['Anio']).mean()
    fig1 = go.Figure()
    fig1.add_trace(go.Line(
    x=pais_lim1.index,
    y=pais_lim1['Consumo_solar'],
    name='Solar',
    marker_color= 'yellow'
        ))
    fig1.add_trace(go.Line(
    x=pais_lim1.index,
    y=pais_lim1['Consumo_Eolica'],
    name='Eolica',
    marker_color='crimson'
        ))
    fig1.add_trace(go.Line(
    x=pais_lim1.index,
    y=pais_lim1['Consumo_Hidroelectrica'],
    name='Hidroelectrica',
    marker_color='blue'
        ))
    fig1.add_trace(go.Line(
    x=pais_lim2.index,
    y=pais_lim2['Consumo_Geotermica'],
    name='Geotermica',
    marker_color='goldenrod'
        ))
    fig1.add_trace(go.Line(
    x=pais_lim2.index,
    y=pais_lim2['Consumo_Nuclear'],
    name='Nuclear',
    marker_color='red'
        ))
    fig1.update_layout(title='Consumo Energia limpia',
                   xaxis_title='Año',
                   yaxis_title='Consumo(Exajoules)')
    return st.plotly_chart(fig1, use_container_width=True)






st.subheader('Paises con mayores emisiones CO2')
genere= st.selectbox(
   'Selecciona el país',
    ('China','United States', 'Russia','Japan','India'))

if genere == 'China':
  st.subheader('China')
  st.write(grafica_consumo_fosil('China'),grafica_consumo_limpio('China'))

elif genere=='United States':
  st.subheader('United States')
  st.write(grafica_consumo_fosil('United States'),grafica_consumo_limpio('United States'))
 
elif genere=='Russia':
  st.subheader('Russia')
  st.write(grafica_consumo_fosil('Russia'),grafica_consumo_limpio('Russia'))
 
elif genere=='Japan':
  st.subheader('Japan')
  st.write(grafica_consumo_fosil('Japan'), grafica_consumo_limpio('Japan'))
  
elif genere=='India':
  st.subheader('India')
  st.write(grafica_consumo_fosil('India'),grafica_consumo_limpio('India'))


st.subheader('Paises con menores emisiones CO2')
genere= st.selectbox(  
  'Selecciona el país',
    ('Morocco','Ireland', 'Peru','Norway','Ecuador'))

if genere=='Morocco':
  st.write(grafica_consumo_fosil('Morocco'),grafica_consumo_limpio('Morocco'))
  
elif genere=='Ireland':
  st.write(grafica_consumo_fosil('Ireland'),grafica_consumo_limpio('Ireland'))
  
elif genere=='Peru':
  st.write(grafica_consumo_fosil('Peru'),grafica_consumo_limpio('Peru'))
  
elif genere=='Norway':
  st.write(grafica_consumo_fosil('Norway'),grafica_consumo_limpio('Norway'))

elif genere=='Ecuador':
  st.write(grafica_consumo_fosil('Ecuador'),grafica_consumo_limpio('Ecuador'))



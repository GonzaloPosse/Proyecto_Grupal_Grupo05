import numpy as np
import pandas as pd
import datetime as dt
from lectura import *
#funcion para  eliminar los nulos en tanblas de irradianza
# se manejan por separado porque las tablas traen varios paises sin registos durante todos  los meses
#se toma la decicion de elmimnar esos registros y conservar los paiese que si contienen info
def trabajar_nullos_irradianza(df):
    return df.dropna(inplace=True)



def trabajar_nulos_ceros_otros(df):
    df.fillna(0, inplace=True)
    df.replace("-", 0, inplace=True)
    df.replace("^", 0, inplace=True)
    df.replace("♦", 0, inplace=True)
    return df

#Funcion para obtener la irradianza por año 
"""def promedio_irradianza(df):
    p=df.mean(axis=1, skipna=True)  
    return np.round(p, decimals=2)"""

def promedio_irradianza(df):

    df['prom']=df.mean(axis=1,numeric_only=True) 

    return np.round(df['prom'], decimals=2)

#funcion para limpieza de tabla poblacion
def limpieza_poblacion(df):
    df.drop(columns=['Indicator Name','Indicator Code'],axis=1,inplace=True)
    df['Country Name']=df['Country Name'].str.upper()
    df.rename(columns={'Country Name':'country_name','Country Code':'country_code'},inplace=True)
    df=normalizar_paises(df)
    lista_bool=comparar_nombre_pais(df)
    lista_falsos=no_paises_filas(lista_bool)
    df= elimimar_filas_incorrectas(lista_falsos,df)
    return df

#funcion para normalizar nombres de paises
def normalizar_paises(df):
    df.replace({'UNITED ARAB EMIRATES': 'UNITED ARAB EMIRATES (THE)',
                      'BAHAMAS, THE':'BAHAMAS (THE)',
                      'BOLIVIA': 'BOLIVIA (PLURINATIONAL STATE OF)',
                      'CENTRAL AFRICAN REPUBLIC':'CENTRAL AFRICAN REPUBLIC (THE)',
                      'CONGO, DEM. REP.':'CONGO (THE DEMOCRATIC REPUBLIC OF THE)',
                      'CONGO, REP.': 'CONGO (THE)',
                      'COMOROS':'COMOROS (THE)',
                      "COTE D'IVOIRE":"CÔTE D'IVOIRE",
                      'CAYMAN ISLANDS':'CAYMAN ISLANDS (THE)',
                      'CZECH REPUBLIC':'CZECHIA',
                      'DOMINICAN REPUBLIC':'DOMINICAN REPUBLIC (THE)',
                      'MICRONESIA, FED. STS.':'MICRONESIA (FEDERATED STATES OF)',
                      'UNITED KINGDOM':'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND (THE)',
                      'HONG KONG SAR':'HONG KONG',
                      'KOREA, REP.':'KOREA (THE REPUBLIC OF)',
                      'ST. LUCIA':'SAINT LUCIA',
                      'SLOVAK REPUBLIC':'SLOVAKIA',
                      'TURKS AND CAICOS ISLANDS':'TURKS AND CAICOS ISLANDS (THE)',
                      'TURKIYE':'TURKEY',
                      'ST. VINCENT AND THE GRENADINES':'SAINT VINCENT AND THE GRENADINES',
                      'BRITISH VIRGIN ISLANDS':'VIRGIN ISLANDS (BRITISH)',
                      'VIETNAM':'VIET NAM',
                      'VENEZUELA, RB':'VENEZUELA (BOLIVARIAN REPUBLIC OF)',
                      'YEMEN, REP.':'YEMEN',
                      'CURACAO':'CURAÇAO',
                      'EGYPT, ARAB REP.':'EGYPT',
                      'FAROE ISLANDS':'FAROE ISLANDS (THE)',
                      'GAMBIA, THE' :'GAMBIA (THE)',
                      'HONG KONG SAR, CHINA': 'HONG KONG',
                      'IRAN, ISLAMIC REP.' :'IRAN (ISLAMIC REPUBLIC OF)',
                      'KYRGYZ REPUBLIC' :'KYRGYZSTAN',
                      'ST. KITTS AND NEVIS' :'SAINT KITTS AND NEVIS',
                      'NORTHERN MARIANA ISLANDS' :'NORTHERN MARIANA ISLANDS (THE)',
                      'NIGER':'NIGER (THE)',
                      'NETHERLANDS':'NETHERLANDS (THE)',
                      'PHILIPPINES' : 'PHILIPPINES (THE)',
                      "KOREA, DEM. PEOPLE'S REP." : "KOREA (THE DEMOCRATIC PEOPLE'S REPUBLIC OF)",
                      'RUSSIAN FEDERATION':'RUSSIAN FEDERATION (THE)',
                      'TANZANIA':'TANZANIA, UNITED REPUBLIC OF',
                      'UNITED STATES':'UNITED STATES OF AMERICA (THE)',
                      'YEMEN, REP.': 'YEMEN',
                      'SUDAN' :'SUDAN (THE)',
                      'MOLDOVA':'MOLDOVA (THE REPUBLIC OF)',
                      'LAO PDR':"LAO PEOPLE'S DEMOCRATIC REPUBLIC (THE)",
                       'MARSHALL ISLANDS':'MARSHALL ISLANDS (THE)'
                      }, inplace=True)
    return df


#funcion para normalizar nombres de paises EM IRRADIANZA
def normalizar_paises_irradianza(df):
    df.replace({'BAHAMAS':'BAHAMAS (THE)',
                    'BOLIVARIAN REPUBLIC OF VENEZUELA' :'VENEZUELA (BOLIVARIAN REPUBLIC OF)',
                    'BONAIRE SAINT EUSTATIUS AND SABA':'BONAIRE, SINT EUSTATIUS AND SABA',
                    'BRITISH INDIAN OCEAN TERRITORY' : 'BRITISH INDIAN OCEAN TERRITORY (THE)',
                    'BRITISH VIRGIN ISLANDS':'VIRGIN ISLANDS (BRITISH)',
                    'CAPE VERDE' : 'CABO VERDE',
                    'CAYMAN ISLANDS': 'CAYMAN ISLANDS (THE)',
                    'CENTRAL AFRICAN REPUBLIC': 'CENTRAL AFRICAN REPUBLIC (THE)',
                    #CHINESE TAIPEI
                    'COMOROS' :'COMOROS (THE)',
                    'COOK ISLANDS':'COOK ISLANDS (THE)',
                    "COTE D'IVOIRE": "CÔTE D'IVOIRE",
                    'CURACAO/NETHERLANDS ANTILLES' :'CURAÇAO',
                    'CZECH REPUBLIC':'CZECHIA',
                    "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA":"KOREA (THE DEMOCRATIC PEOPLE'S REPUBLIC OF)",
                    'DEMOCRATIC REPUBLIC OF THE CONGO':'CONGO (THE DEMOCRATIC REPUBLIC OF THE)',
                    'DOMINICAN REPUBLIC':'DOMINICAN REPUBLIC (THE)',
                    'FAEROE ISLANDS':'FAROE ISLANDS (THE)',
                    'FALKLAND ISLANDS (MALVINAS)':"FALKLAND ISLANDS (THE) [MALVINAS]",
                    'FRENCH SOUTHERN TERRITORIES':'FRENCH SOUTHERN TERRITORIES (THE)',
                    'GAMBIA':'GAMBIA (THE)',
                    'HONG KONG (CHINA)':'HONG KONG',
                    'ISLAMIC REPUBLIC OF IRAN':'IRAN (ISLAMIC REPUBLIC OF)',
                    'KOREA':'KOREA (THE REPUBLIC OF)',
                    #KOSOVO
                    "LAO PEOPLE'S DEMOCRATIC REPUBLIC":"LAO PEOPLE'S DEMOCRATIC REPUBLIC (THE)",
                    'MARSHALL ISLANDS':'MARSHALL ISLANDS (THE)',
                    'NETHERLANDS' :'NETHERLANDS (THE)',
                    'NIGER':'NIGER (THE)',
                    'NORTHERN MARIANA ISLANDS':'NORTHERN MARIANA ISLANDS (THE)',
                    #PEOPLE'S REPUBLIC OF CHINA
                    'PHILIPPINES':'PHILIPPINES (THE)',
                    'PLURINATIONAL STATE OF BOLIVIA':'BOLIVIA (PLURINATIONAL STATE OF)',
                    'REPUBLIC OF MOLDOVA':'MOLDOVA (THE REPUBLIC OF)',
                    'REPUBLIC OF THE CONGO':'CONGO (THE DEMOCRATIC REPUBLIC OF THE)',
                    'RUSSIAN FEDERATION':'RUSSIAN FEDERATION (THE)',
                    'SAINT HELENA':'SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA',
                    'SAINT-BARTHELEMY':'SAINT BARTHELEMY',
                    'SLOVAK REPUBLIC':'SLOVAKIA',
                    # SPRATLY ISLANDS
                    'SUDAN':'SUDAN (THE)',
                    'SVALBARD AND JAN MAYEN ISLANDS':'SVALBARD AND JAN MAYEN',
                    'SWAZILAND':'ESWATINI'
                    # MORDOVIA
                      }, inplace=True)
    return df
#funcion para comparar los nombres de paises en el dataframe siempre debe llamarse la columna country_name
def comparar_nombre_pais(df):
        a=df.country_name.isin(['AFGHANISTAN', 'ALAND ISLANDS', 'ALBANIA', 'ALGERIA', 'AMERICAN SAMOA',
                                        'ANDORRA', 'ANGOLA', 'ANGUILLA', 'ANTARCTICA', 'ANTIGUA AND BARBUDA',
                                        'ARGENTINA', 'ARMENIA','ARUBA', 'AUSTRALIA', 'AUSTRIA','AZERBAIJAN',
                                        'BAHAMAS (THE)', 'BAHRAIN', 'BANGLADESH','BARBADOS', 'BELARUS', 'BELGIUM',
                                        'BELIZE', 'BENIN', 'BERMUDA', 'BHUTAN', 'BOLIVIA (PLURINATIONAL STATE OF)',
                                        'BONAIRE, SINT EUSTATIUS AND SABA', 'BOSNIA AND HERZEGOVINA', 'BOTSWANA',
                                        'BOUVET ISLAND', 'BRAZIL', 'BRITISH INDIAN OCEAN TERRITORY (THE)',
                                        'BRUNEI DARUSSALAM', 'BULGARIA', 'BURKINA FASO', 'BURUNDI', 'CABO VERDE',
                                        'CAMBODIA', 'CAMEROON','CANADA', 'CAYMAN ISLANDS (THE)',
                                        'CENTRAL AFRICAN REPUBLIC (THE)', 'CHAD', 'CHILE', 'CHINA',
                                        'CHRISTMAS ISLAND', 'COCOS (KEELING) ISLANDS (THE)', 'COLOMBIA',
                                        'COMOROS (THE)', 'CONGO (THE DEMOCRATIC REPUBLIC OF THE)', 'CONGO (THE)',
                                        'COOK ISLANDS (THE)', 'COSTA RICA', "CÔTE D'IVOIRE", 'CROATIA', 'CUBA',
                                        'CURAÇAO', 'CYPRUS', 'CZECHIA', 'DENMARK', 'DJIBOUTI', 'DOMINICA',
                                        'DOMINICAN REPUBLIC (THE)', 'ECUADOR', 'EGYPT', 'EL SALVADOR',
                                        'EQUATORIAL GUINEA', 'ERITREA', 'ESTONIA', 'ESWATINI', 'ETHIOPIA',
                                        'FALKLAND ISLANDS (THE) [MALVINAS]', 'FAROE ISLANDS (THE)', 'FIJI',
                                        'FINLAND','FRANCE', 'FRENCH GUIANA', 'FRENCH POLYNESIA',
                                        'FRENCH SOUTHERN TERRITORIES (THE)', 'GABON','GAMBIA (THE)', 'GEORGIA',
                                        'GERMANY','GHANA','GIBRALTAR', 'GREECE', 'GREENLAND','GRENADA', 'GUADELOUPE',
                                        'GUAM', 'GUATEMALA', 'GUERNSEY', 'GUINEA', 'GUINEA-BISSAU', 'GUYANA', 'HAITI',
                                        'HEARD ISLAND AND MCDONALD ISLANDS', 'HOLY SEE (THE)', 'HONDURAS',
                                        'HONG KONG', 'HUNGARY', 'ICELAND', 'INDIA', 'INDONESIA',
                                        'IRAN (ISLAMIC REPUBLIC OF)', 'IRAQ', 'IRELAND', 'ISLE OF MAN', 'ISRAEL',
                                        'ITALY', 'JAMAICA', 'JAPAN', 'JERSEY','JORDAN', 'KAZAKHSTAN', 'KENYA',
                                        'KIRIBATI', "KOREA (THE DEMOCRATIC PEOPLE'S REPUBLIC OF)",
                                        'KOREA (THE REPUBLIC OF)', 'KUWAIT', 'KYRGYZSTAN',
                                        "LAO PEOPLE'S DEMOCRATIC REPUBLIC (THE)", 'LATVIA', 'LEBANON', 'LESOTHO',
                                        'LIBERIA', 'LIBYA', 'LIECHTENSTEIN','LITHUANIA', 'LUXEMBOURG', 'MACAO',
                                        'REPUBLIC OF NORTH MACEDONIA', 'MADAGASCAR', 'MALAWI', 'MALAYSIA', 'MALDIVES',
                                        'MALI', 'MALTA', 'MARSHALL ISLANDS (THE)', 'MARTINIQUE', 'MAURITANIA',
                                        'MAURITIUS','MAYOTTE','MEXICO', 'MICRONESIA (FEDERATED STATES OF)',
                                        'MOLDOVA (THE REPUBLIC OF)', 'MONACO', 'MONGOLIA', 'MONTENEGRO', 'MONTSERRAT',
                                        'MOROCCO','MOZAMBIQUE', 'MYANMAR', 'NAMIBIA', 'NAURU', 'NEPAL',
                                        'NETHERLANDS (THE)', 'NEW CALEDONIA', 'NEW ZEALAND', 'NICARAGUA',
                                        'NIGER (THE)', 'NIGERIA','NIUE', 'NORFOLK ISLAND',
                                        'NORTHERN MARIANA ISLANDS (THE)','NORWAY', 'OMAN','PAKISTAN', 'PALAU',
                                        'PALESTINE, STATE OF', 'PANAMA', 'PAPUA NEW GUINEA','PARAGUAY', 'PERU',
                                        'PHILIPPINES (THE)', 'PITCAIRN','POLAND', 'PORTUGAL', 'PUERTO RICO', 'QATAR',
                                        'REUNION', 'ROMANIA', 'RUSSIAN FEDERATION (THE)', 'RWANDA',
                                        'SAINT BARTHELEMY','SAINT HELENA, ASCENSION AND TRISTAN DA CUNHA',
                                        'SAINT KITTS AND NEVIS', 'SAINT LUCIA','SAINT MARTIN (FRENCH PART)',
                                        'SAINT PIERRE AND MIQUELON', 'SAINT VINCENT AND THE GRENADINES', 'SAMOA',
                                        'SAN MARINO', 'SAO TOME AND PRINCIPE', 'SAUDI ARABIA', 'SENEGAL', 'SERBIA',
                                        'SEYCHELLES', 'SIERRA LEONE', 'SINGAPORE', 'SINT MAARTEN (DUTCH PART)',
                                        'SLOVAKIA', 'SLOVENIA','SOLOMON ISLANDS', 'SOMALIA', 'SOUTH AFRICA',
                                        'SOUTH GEORGIA AND THE SOUTH SANDWICH ISLANDS', 'SOUTH SUDAN','SPAIN',
                                        'SRI LANKA', 'SUDAN (THE)','SURINAME', 'SVALBARD AND JAN MAYEN', 'SWEDEN',
                                        'SWITZERLAND','SYRIAN ARAB REPUBLIC','TAIWAN (PROVINCE OF CHINA)',
                                        'TAJIKISTAN', 'TANZANIA, UNITED REPUBLIC OF', 'THAILAND','TIMOR-LESTE',
                                        'TOGO', 'TOKELAU', 'TONGA', 'TRINIDAD AND TOBAGO','TUNISIA', 'TURKEY',
                                        'TURKMENISTAN', 'TURKS AND CAICOS ISLANDS (THE)', 'TUVALU', 'UGANDA',
                                        'UKRAINE', 'UNITED ARAB EMIRATES (THE)',
                                        'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND (THE)',
                                        'UNITED STATES MINOR OUTLYING ISLANDS (THE)',
                                        'UNITED STATES OF AMERICA (THE)', 'URUGUAY','UZBEKISTAN', 'VANUATU',
                                        'VENEZUELA (BOLIVARIAN REPUBLIC OF)', 'VIET NAM',
                                        'VIRGIN ISLANDS (BRITISH)', 'VIRGIN ISLANDS (U.S.)', 'WALLIS AND FUTUNA',
                                        'WESTERN SAHARA', 'YEMEN', 'ZAMBIA', 'ZIMBABWE'])
        return a

#funcion para obtener la lista de los valores que no son paises
def no_paises_filas(a):
    #p=df.country_name
    falsos = []
    for num, i in enumerate(a):
        if i==False:
             #cf+=1
            falsos.append(num)
            #print(f'{num} : {i}: {p[num]}') 
    return falsos


def elimimar_filas_incorrectas(falsos,df):
    for i in falsos:
         df.drop([i], axis=0, inplace=True)
    return df 

def limpiando_pais(url):
    #url = 'https://www.iban.com/country-codes'
    paises=obtener_paises_codigo(url)
    paises.replace({'ÅLAND ISLANDS': 'ALAND ISLANDS',
                    'SAINT BARTHÉLEMY':'SAINT BARTHELEMY',
                    'RÉUNION': 'REUNION'}, inplace=True)
    paises.insert(0, 'id_country', range(1, 1+ len(paises)))
    paises['id_country']=paises['id_country'].astype(int)
    return paises
           
#funcion para llenar la columna id_country segun el country_name de cada df
def agregar_id_pais(df_paises,df): 
    for i,p in enumerate(df_paises['country_name']):
        cod= df_paises.loc[i,'id_country']
        df.loc[df['country_name']==p,'id_country']=cod
    #print(df['country_name'])
    df['id_country']=df['id_country'].astype(int)
    df.drop(['country_name'], axis=1,inplace=True)
    #df.sort_values(by=['id_country'],axis=0, ascending=True)
    return df

def agregar_id_pais_poblacion(df_paises,df): 
    for i,p in enumerate(df_paises['country_name']):
        cod= df_paises.loc[i,'id_country']
        df.loc[df['country_name']==p,'id_country']=cod
    df['id_country']=df['id_country'].astype(int)
    df.drop(['country_name','country_code'], axis=1,inplace=True)

    return df    

def tabla_años():
    a=np.arange(1960,2023)
    df_años=pd.DataFrame({'year':a})
    df_años.insert(0, 'id_year', range(1, 1+ len(df_años)))
    df_años['year']=df_años['year'].astype(str)
    return df_años

#funcion para crear el catalogo de enrgias, es posible actualziarlo, colococando unicamente el nuevo tipo de energi en la lista
def catalogo_energia():
    energia=['SOLAR','WIND','COAL','OIL','NUCLEAR','NATURAL GAS','GEOTHERMAL','HIDROELECTRIC']
    id=range(1,1+len(energia))
    df_energia=pd.DataFrame({'id_energy':id,
                              'energy_name':energia})  
    return df_energia  

def lectura_irra(urls):
    
    #recorremos todas las url y generemaos el dataframe con todos los alos hasta el 2022 el cual solo tiene datos registrados hasta julio 2022
    contador=2010
    for i,url in enumerate (urls):
        d = lectura_archivos(url)        
        #d = trabajar_nullos_irradianza(d)
        d= trabajar_nulos_ceros_otros(d)
        indices=d[d['enero']== 0.0].index
        d.drop(indices,inplace=True)
        d.reset_index(inplace=True)
        d.drop(['index'],axis=1, inplace=True)
       
        if i==0:
            df_irra =pd.DataFrame({'pais':d['pais']})#creamos un nuevo dataframe con su primera columna nombres de paises
            df_irra[contador]=promedio_irradianza(d)
            contador+=1
        elif i>=1:
            df_irra[contador]= promedio_irradianza(d)
            contador+=1 
    return  df_irra  

def limpieza_irradianza(df_irra):  
   
    df_irra['pais']=df_irra['pais'].str.upper()
    df_irra.rename(columns={'pais':'country_name'}, inplace=True)
    #compara_paises=comparar_nombre_pais(df_irra)
    #falsos_total=no_paises_filas(compara_paises)
    df_irra_final=normalizar_paises_irradianza(df_irra)
    df_irra_final.reset_index(inplace=True)
    df_irra_final.drop(['index'],axis=1,inplace=True)
    f=comparar_nombre_pais(df_irra_final)
    lista_falso=no_paises_filas(f)
    df_irra_final=elimimar_filas_incorrectas(lista_falso,df_irra_final)
    
    return df_irra_final

def agregar_id_propio_tabla(df,nombre_campo):
    df.insert(0, nombre_campo, range(1, 1+ len(df)))  
    return df  

#funcion mapeo de nombres de columnas por datos incosistentes
def mapeo_colum_años(df_irra_final):    
    df_irra_final=df_irra_final.rename({2010:'2010', 2011:'2011', 2012:'2012',2013:'2013',2014:'2014',
               2015:'2015', 2016:'2016',2017:'2017',2018:'2018',2019:'2019',
               2020:'2020',2021:'2021', 2022:'2022', 'id_country':'id_country'},axis='columns')
    return(df_irra_final)


#funcion para cambiar el numero de año por el id que le corresponde
def agregar_cod_año(df_años,df): 
    for i,p in enumerate(df_años['year']):
        cod= df_años.loc[i,'id_year']
        df.loc[df['year']==p,'id_year']=cod
    df['id_year']=df['id_year'].astype(int)
    df.drop(['year'],axis=1,inplace=True)    
    return df  
   

def funcion_reshape(df):
    a=list(np.arange(2010,2023))
    a=list(map(str,a))
    df= pd.melt(df, id_vars='id_country',value_vars=a,var_name='year',value_name='annual_irradiation')
    
    return  df 

#podria servir para todas las otras tablas 
def reshape_poblacion(df, nombre_campo):
    c=df.columns
    inicio=int(c[0])
    #print(c[-2])
    #print(type(c[-2]))
    fin=int(c[-2])+1       
    a=list(np.arange(inicio,fin))
    a=list(map(str,a))
    df=pd.melt(df, id_vars='id_country',value_vars=a,var_name='year',value_name= nombre_campo)
    #df=df.astype({nombre_campo:'int64'})
    #df[nombre_campo]=df[nombre_campo].astype(int)
    return df



"""
    # Funcion para corregir paises por escritura incorrecta
def paises_map(df):
    df['Pais'] = df['Pais'].map({'Denmark': 'Denmark',
                                     'US': 'United States', 'Germany': 'Germany', 'Netherlands': 'Netherlands',
                                     'India': 'India', 'Canada': 'Canada',
                                     'Spain': 'Spain', 'China': 'China', 'Sweden': 'Sweden',
                                     'United Kingdom': 'United Kingdom', 'Mexico': 'Mexico',
                                     'Argentina': 'Argentina', 'Brazil': 'Brazil',
                                     'Central America': 'Other South and Central America (BP)'
                                        , 'Austria': 'Austria', 'Belgium': 'Belgium',
                                     'Finland': 'Finland', 'France': 'France', 'Greece': 'Greece', 'Ireland': 'Ireland',
                                     'Italy': 'Italy', 'Norway': 'Norway',
                                     'Poland': 'Poland', 'Portugal': 'Portugal', 'Turkey': 'Turkey',
                                     'Other Europe': 'Other Europe (BP)', 'Iran': 'Iran', 'Egypt': 'Egypt',
                                     'Australia': 'Australia', 'Japan': 'Japan', 'New Zealand': 'New Zealand',
                                     'South Korea': 'South Korea', 'Taiwan': 'Taiwan',
                                     'Other Asia Pacific': 'Other Asia Pacific (BP)', 'Morocco': 'Morocco',
                                     'Ukraine': 'Ukraine', 'Russian Federation': 'Russia',
                                     'Chile': 'Chile', 'Other CIS': 'Other CIS (BP)',
                                     'Other Caribbean': 'Other Caribbean (BP)',
                                     'Other Middle East': 'Other Middle East (BP)',
                                     'South Africa': 'South Africa', 'Colombia': 'Colombia', 'Bulgaria': 'Bulgaria',
                                     'Estonia': 'Estonia', 'Luxembourg': 'Luxembourg',
                                     'Romania': 'Romania', 'Philippines': 'Philippines', 'Czech Republic': 'Czechia',
                                     'Hungary': 'Hungary', 'Latvia': 'Latvia',
                                     'Lithuania': 'Lithuania', 'Thailand': 'Thailand', 'Pakistan': 'Pakistan',
                                     'Croatia': 'Croatia',
                                     'Other Northern Africa': 'Other Northern Africa (BP)',
                                     'Other South America': 'Other South America (BP)', 'Sri Lanka': 'Sri Lanka',
                                     'Cyprus': 'Cyprus', 'Switzerland': 'Switzerland',
                                     'Eastern Africa': 'Eastern Africa (BP)',
                                     'Western Africa': 'Other Western Africa (BP)',
                                     'Ecuador': 'Ecuador', 'Venezuela': 'Venezuela', 'Vietnam': 'Vietnam',
                                     'Peru': 'Peru', 'North Macedonia': 'North Macedonia',
                                     'Kazakhstan': 'Kazakhstan', 'Belarus': 'Belarus', 'Azerbaijan': 'Azerbaijan',
                                     'Indonesia': 'Indonesia', 'Oman': 'Oman',
                                     'Costa Rica': 'Costa Rica',
                                     'Other S. & Cent. America': 'Other South and Central America (BP)',
                                     'Tunisia': 'Tunisia', 'Jordan': 'Jordan',
                                     'Other Africa': 'Other Africa (BP)', 'Uruguay': 'Uruguay'})
    return df
    """
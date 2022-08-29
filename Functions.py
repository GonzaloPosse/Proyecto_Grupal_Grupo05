import pandas as pd

def formato(df):
    df.fillna(0, inplace=True)
    df.replace("-",0, inplace=True)
    df.replace("^",0, inplace=True)
    return df
def lectura_csv(url):
    df=pd.read_csv(url)
    return df
def cambiar_nobre_x_id_años(df_años,df):
    for i,e in enumerate(df_años['Anio']):
        id= df_años.loc[i,'Id_Años']
        df.loc[df['Año']==e,'Id_anio']=id
    return df
def llenar_Id_Pais(df_paises,df): 
    for i,p in enumerate(df_paises['Pais']):
        cod= df_paises.loc[i,'Id_pais']
        df.loc[df['Pais']==p,'Id_Pais']=cod
    return df

def paises_map(df):
      df['Pais']=df['Pais'].map({'Denmark':'Denmark', 
            'US':'United States', 'Germany':'Germany', 'Netherlands':'Netherlands',
            'India':'India', 'Canada':'Canada',
            'Spain':'Spain', 'China':'China', 'Sweden':'Sweden', 'United Kingdom':'United Kingdom', 'Mexico':'Mexico',
            'Argentina':'Argentina', 'Brazil':'Brazil', 'Central America':'Other South and Central America (BP)'
            , 'Austria':'Austria', 'Belgium':'Belgium',
            'Finland':'Finland', 'France':'France', 'Greece':'Greece', 'Ireland':'Ireland', 'Italy':'Italy', 'Norway':'Norway',
            'Poland':'Poland', 'Portugal':'Portugal', 'Turkey':'Turkey', 'Other Europe':'Other Europe (BP)', 'Iran':'Iran', 'Egypt':'Egypt',
            'Australia':'Australia', 'Japan':'Japan', 'New Zealand':'New Zealand', 'South Korea':'South Korea', 'Taiwan':'Taiwan',
            'Other Asia Pacific':'Other Asia Pacific (BP)', 'Morocco':'Morocco', 'Ukraine':'Ukraine', 'Russian Federation':'Russia',
            'Chile':'Chile', 'Other CIS':'Other CIS (BP)', 'Other Caribbean':'Other Caribbean (BP)', 'Other Middle East':'Other Middle East (BP)',
            'South Africa':'South Africa', 'Colombia':'Colombia', 'Bulgaria':'Bulgaria', 'Estonia':'Estonia', 'Luxembourg':'Luxembourg',
            'Romania':'Romania', 'Philippines':'Philippines', 'Czech Republic':'Czechia', 'Hungary':'Hungary', 'Latvia':'Latvia',
            'Lithuania':'Lithuania', 'Thailand':'Thailand', 'Pakistan':'Pakistan', 'Croatia':'Croatia',
            'Other Northern Africa':'Other Northern Africa (BP)', 'Other South America':'Other South America (BP)', 'Sri Lanka':'Sri Lanka',
            'Cyprus':'Cyprus', 'Switzerland':'Switzerland', 'Eastern Africa':'Eastern Africa (BP)', 'Western Africa':'Other Western Africa (BP)',
            'Ecuador':'Ecuador', 'Venezuela':'Venezuela', 'Vietnam':'Vietnam', 'Peru':'Peru', 'North Macedonia': 'North Macedonia',
            'Kazakhstan':'Kazakhstan', 'Belarus':'Belarus', 'Azerbaijan':'Azerbaijan', 'Indonesia':'Indonesia', 'Oman':'Oman',
            'Costa Rica':'Costa Rica', 'Other S. & Cent. America':'Other South and Central America (BP)', 'Tunisia':'Tunisia', 'Jordan':'Jordan',
            'Other Africa':'Other Africa (BP)', 'Uruguay':'Uruguay'})
      return df
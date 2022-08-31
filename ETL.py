import Functions
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
# Conexion a la base de datos en RDS
clave = str(input('Ingrese la conexion a la base de datos: '))
conexion = create_engine(clave)
if conexion.connect():
    print('Conexion a la base de datos con exito')
años=list(np.arange(1980,2021))
años=list(map(str,años))

"""
    Gas Natural, Dev: Gonzalo Posse
    Lectura de csv
"""

reserva_gas = Functions.lectura_csv('D:\data\Proyectos\Grupal\Grupal-DTS02\proyecto_grupal_consumo_energ-a_co2\datasets\Reserva de gas natural Trillion billones metros cubicos.csv')
produccion_gas = Functions.lectura_csv('D:\data\Proyectos\Grupal\Grupal-DTS02\proyecto_grupal_consumo_energ-a_co2\datasets\Produccion Gas Natural Exajoules.csv')
consumo_gas = Functions.lectura_csv('D:\data\Proyectos\Grupal\Grupal-DTS02\proyecto_grupal_consumo_energ-a_co2\datasets\Consumo Gas Natural Exajoules.csv')

"""
    Proceso de transformacion
"""
reserva_gas.drop(["2020.1", "2009-19", "2020.2"], axis=1, inplace=True)
produccion_gas.drop(["2021.1", "2011-21", "2021.2",'2021'], axis=1, inplace=True)
consumo_gas.drop(["2021.1", "2011-21", "2021.2",'2021'], axis=1, inplace=True)

reserva_gas_df = Functions.formato(reserva_gas)
produccion_gas_df = Functions.formato(reserva_gas)
consumo_gas_df = Functions.formato(consumo_gas)

reserva_gas_df = pd.melt(reserva_gas_df, id_vars='Pais',  value_vars=años, var_name="Año", value_name="Reserva de gas")
produccion_gas_df = pd.melt(produccion_gas_df, id_vars='Pais',  value_vars=años, var_name="Año", value_name="Produccion de gas")
consumo_gas_df = pd.melt(consumo_gas_df, id_vars='Pais',  value_vars=años, var_name="Año", value_name="Consumo de gas")

gas1 = pd.merge(reserva_gas_df, produccion_gas_df)
gas_final = pd.merge(gas1, consumo_gas_df)

paises= Functions.lectura_csv('D:\data\Proyectos\Grupal\Grupal-DTS02\proyecto_grupal_consumo_energ-a_co2\datasets\Codigo_pais.csv')
paises.insert(0, 'Id_pais', range(1, 1+ len(paises)))
gas_final = Functions.llenar_Id_Pais(paises,gas_final)
gas_final = Functions.paises_map(gas_final)
anios = Functions.lectura_csv('D:\data\Proyectos\Grupal\Grupal-DTS02\proyecto_grupal_consumo_energ-a_co2\datasets\years.csv')
anios.insert(0, 'Id_Años', range(1, 1+ len(anios)))

gas_final.insert(1,column='Id_Pais',value=0)
gas_final.insert(3,column='Id_anio',value=0)
gas_final = gas_final.astype({'Año':'int64'})

gas_final = Functions.cambiar_nobre_x_id_años(anios,gas_final)
gas_final.drop(["Pais", "Año"], axis=1, inplace=True)


gas_final = gas_final.rename({'Produccion de gas':'Produccion_gas','Consumo de gas':'Consumo_gas','Reserva de gas':'Reserva_Gas'},axis=1)
gas_final = gas_final.astype({'Produccion_gas':float,'Reserva_Gas':float,'Consumo_gas':float})
gas_final.insert(0, 'ID_Gas_Natural', range(1, 1+ len(gas_final)))

"""
    Emisiones de CO2, Dev: Franco Ramseyer
    Lectura de csv
"""

Emisiones_CO2=Functions.lectura_csv('C:\Users\franc\OneDrive\Documentos\Henry\Proyecto grupal\Otros\Proyecto final Henry Generacion CO2\datasets\Emisiones de dioxido de carbono totales.csv')

"""
    Proceso de transformacion
"""

Emisiones_CO2.drop(["2021.1", "2011-21", "2021.2"], axis=1, inplace=True)
Emisiones_CO2.dropna(inplace=True)
años=list(np.arange(1990,2022))
años=list(map(str,años))
Emisiones_CO2 = pd.melt(Emisiones_CO2, id_vars='Pais', value_vars=años, var_name="Año", value_name= "Emisiones_CO2")
Emisiones_CO2.insert(0, 'ID_Emisiones_CO2', range(1, 1+ len(Emisiones_CO2)))
anios.insert(0, 'Id_Años', range(1, 1+ len(anios)))
Emisiones_CO2.insert(1,column='Id_Pais',value=0)
Emisiones_CO2.insert(3,column='Id_anio',value=0)
Emisiones_CO2 = Emisiones_CO2.astype({'Año':'int64'})
Emisiones_CO2=Functions.cambiar_nobre_x_id_años(anios,Emisiones_CO2)
Emisiones_CO2=Functions.paises_map(Emisiones_CO2)
Emisiones_CO2=Functions.llenar_Id_Pais(paises,Emisiones_CO2)
Emisiones_CO2.drop(["Pais", "Año"], axis=1, inplace=True)

"""
    Petróleo, Dev: Franco Ramseyer
    Lectura de csv
"""

consumopetroleo = Functions.lectura_csv('C:\Users\franc\OneDrive\Documentos\Henry\Proyecto grupal\Otros\Proyecto final Henry Generacion CO2\datasets\Consumo Petroleo Barril Miles Diarios.csv')
produccionpetroleo = Functions.lectura_csv('C:\Users\franc\OneDrive\Documentos\Henry\Proyecto grupal\Otros\Proyecto final Henry Generacion CO2\datasets\Produccion Petroleo Barriles Milles Diarios.csv')
reservaspetroleo = Functions.lectura_csv('C:\Users\franc\OneDrive\Documentos\Henry\Proyecto grupal\Otros\Proyecto final Henry Generacion CO2\datasets\Reservas Petroleo Provadas  Barriles en miles de millones .csv')

"""
    Proceso de transformacion
"""

consumopetroleo = Functions.formato(consumopetroleo)
produccionpetroleo = Functions.formato(produccionpetroleo)
reservaspetroleo = Functions.formato(reservaspetroleo)

consumopetroleo.drop(["2021.1", "2011-21", "2021.2"], axis=1, inplace=True)
produccionpetroleo.drop(["2021.1", "2011-21", "2021.2"], axis=1, inplace=True)
reservaspetroleo.drop(["2020.1", "2009-19", "2020.2"], axis=1, inplace=True)
años=list(np.arange(1965,2022))
años=list(map(str,años))
consumopetroleo = pd.melt(consumopetroleo, id_vars='Pais', value_vars=años, var_name="Año", value_name= "Consumo_Petroleo")
produccionpetroleo = pd.melt(produccionpetroleo, id_vars='Pais', value_vars=años, var_name="Año", value_name= "Produccion_Petroleo")
años=list(np.arange(1980,2021))
años=list(map(str,años))
reservaspetroleo = pd.melt(reservaspetroleo, id_vars='Pais', value_vars=años, var_name="Año", value_name= "Reservas_Petroleo")

petroleo1 = pd.merge(consumopetroleo, produccionpetroleo)
Energia_petroleo = pd.merge(petroleo1, reservaspetroleo, how="outer")
Energia_petroleo.fillna(0, inplace=True)
Energia_petroleo

Energia_petroleo.insert(1,column='Id_Pais',value=0)
Energia_petroleo.insert(3,column='Id_anio',value=0)

Energia_petroleo = Energia_petroleo.astype({'Año':'int64'})
Energia_petroleo=Functions.cambiar_nobre_x_id_años(anios,Energia_petroleo)
Energia_petroleo=Functions.paises_map(Energia_petroleo)
Energia_petroleo=Functions.llenar_Id_Pais(paises,Energia_petroleo)
Energia_petroleo.insert(0, 'ID_Energia_petroleo', range(1, 1+ len(Energia_petroleo)))
Energia_petroleo.drop(["Pais", "Año"], axis=1, inplace=True)
Energia_petroleo = Energia_petroleo.astype({'Consumo_Petroleo': int, "Produccion_Petroleo": int, "Reservas_Petroleo": float})

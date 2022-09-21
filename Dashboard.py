{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "collapsed_sections": [],
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/GonzaloPosse/Proyecto_Grupal_Grupo05/blob/main/Dashboard.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "pip install streamlit -q"
      ],
      "metadata": {
        "id": "Cwh5e7x6xGvZ",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "f272d886-e47e-4862-9c0f-893aa095f8eb"
      },
      "execution_count": 1,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "\u001b[K     |████████████████████████████████| 9.1 MB 5.2 MB/s \n",
            "\u001b[K     |████████████████████████████████| 78 kB 6.6 MB/s \n",
            "\u001b[K     |████████████████████████████████| 164 kB 14.5 MB/s \n",
            "\u001b[K     |████████████████████████████████| 181 kB 38.6 MB/s \n",
            "\u001b[K     |████████████████████████████████| 235 kB 32.9 MB/s \n",
            "\u001b[K     |████████████████████████████████| 4.7 MB 33.9 MB/s \n",
            "\u001b[K     |████████████████████████████████| 63 kB 1.3 MB/s \n",
            "\u001b[K     |████████████████████████████████| 51 kB 5.4 MB/s \n",
            "\u001b[?25h  Building wheel for validators (setup.py) ... \u001b[?25l\u001b[?25hdone\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 2,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "NenV8czv8ymu",
        "outputId": "7cffdb6c-2c07-4d2d-ba7e-00e539092d23"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Looking in indexes: https://pypi.org/simple, https://us-python.pkg.dev/colab-wheels/public/simple/\n",
            "Collecting pyngrok\n",
            "  Downloading pyngrok-5.1.0.tar.gz (745 kB)\n",
            "\u001b[K     |████████████████████████████████| 745 kB 4.9 MB/s \n",
            "\u001b[?25hRequirement already satisfied: PyYAML in /usr/local/lib/python3.7/dist-packages (from pyngrok) (6.0)\n",
            "Building wheels for collected packages: pyngrok\n",
            "  Building wheel for pyngrok (setup.py) ... \u001b[?25l\u001b[?25hdone\n",
            "  Created wheel for pyngrok: filename=pyngrok-5.1.0-py3-none-any.whl size=19007 sha256=98a9ce5960eb12e72d9b56a0ddef6cff01bb5485062df4a8d77a483bcdb1bb31\n",
            "  Stored in directory: /root/.cache/pip/wheels/bf/e6/af/ccf6598ecefecd44104069371795cb9b3afbcd16987f6ccfb3\n",
            "Successfully built pyngrok\n",
            "Installing collected packages: pyngrok\n",
            "Successfully installed pyngrok-5.1.0\n"
          ]
        }
      ],
      "source": [
        "! pip install pyngrok"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "%%writefile app.py\n",
        "import streamlit as st\n",
        "from PIL import Image\n",
        "import plotly.figure_factory as ff\n",
        "import numpy as np\n",
        "import plotly.express as px\n",
        "import pandas as pd\n",
        "from sqlalchemy import create_engine\n",
        "import plotly.graph_objects as go\n",
        "import folium\n",
        "\n",
        "conexion= create_engine('postgresql://{user}:{pw}@{host}:{port}/{db}'.format(\n",
        "    user='postgres', pw='postgres', host='database-grupo5.cgmzd7suyc4v.us-east-1.rds.amazonaws.com',port='5432',db='postgres', echo=False))\n",
        "\n",
        "\n",
        "st.set_page_config(page_title='Equipo 5 - Henry')\n",
        "st.title('Proyecto Grupal - Equipo 5')\n",
        "\n",
        "st.sidebar.header('Sectores')\n",
        "st.sidebar.selectbox('Selecciona la gráfica de tu interés',['CO2','Energías Renovables','Modelo de Machine Learning'])\n",
        "\n",
        "\n",
        "\n",
        "\n",
        "paises = pd.read_sql_query('SELECT * FROM \"Paises\"',con=conexion)\n",
        "anos = pd.read_sql_query('SELECT * FROM \"Años\"' ,con=conexion)\n",
        "emisiones_sin_año = pd.read_sql_query ('SELECT * FROM \"Emisiones_CO2\"',con=conexion)\n",
        "nuclear_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_nuclear\"',con=conexion)\n",
        "eolica_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_nuclear\"',con=conexion)\n",
        "geotermica_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_Geotermica\"',con=conexion)\n",
        "solar_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_solar\"',con=conexion)\n",
        "Energias_Renovables_Fer = pd.read_sql_query ('SELECT * FROM \"Energias_Renovables_Fer\"',con=conexion)\n",
        "er_fer = pd.read_sql_query ('SELECT * FROM \"er_fer\"',con=conexion)\n",
        "# Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi # Javi  \n",
        "\n",
        "df_paises_j = pd.read_sql_query('SELECT * FROM \"Paises\"',con=conexion)\n",
        "df_Años_j = pd.read_sql_query('SELECT * FROM \"Años\"',con=conexion)\n",
        "df_emisiones_CO2_j = pd.read_sql_query('SELECT * FROM \"Emisiones_CO2\"',con=conexion)\n",
        "df_poblacion_j = pd.read_sql_query('SELECT * FROM \"Poblacion_Mundo\"',con=conexion)\n",
        "df_consumo = pd.read_sql_query('SELECT * FROM \"Energia_total\"',con=conexion)\n",
        "df_poblacion_j = df_poblacion_j.rename(columns={'Id_Anio':'Id_anio'})\n",
        "\n",
        "df = df_emisiones_CO2_j.merge(df_poblacion_j, on=['Id_Pais','Id_anio'], how='left').dropna()\n",
        "df = df.merge(df_consumo, on=['Id_Pais','Id_anio'], how='left').dropna()\n",
        "df = df.merge(df_paises_j, on='Id_Pais', how='left')\n",
        "df = df.merge(df_Años_j, on='Id_anio', how='left')\n",
        "df = df.drop(columns=['ID_Emisiones_CO2','Id_Pais','Id_anio','Id_Poblacion','ID_consumototal'])\n",
        "df = df.reindex(columns=['Codigo_pais','Pais','Anio','Poblacion','Emisiones_CO2','Consumo_total'])\n",
        "df = df.rename(columns={'Pais':'País','Anio':'Año','Emisiones_CO2':'Emisiones CO2','Consumo_total':'Consumo Energía','Poblacion':'Población'})\n",
        "\n",
        "\n",
        "# MAPA DE EMISIONES HISTORICAS\n",
        "\n",
        "min_value = df['Emisiones CO2'].min()\n",
        "max_value = df['Emisiones CO2'].max()\n",
        "fig = px.choropleth(df, locations=\"Codigo_pais\",\n",
        "                    color=\"Emisiones CO2\",\n",
        "                    locationmode=\"ISO-3\",\n",
        "                    animation_frame='Año',\n",
        "                    color_continuous_midpoint = 3,\n",
        "color_continuous_scale=px.colors.sequential.thermal_r,\n",
        "range_color=(min_value,max_value))\n",
        "fig.update_layout(width=800, height=500, title_text = 'Emisiones históricas de CO2',font_size=14)\n",
        "st.plotly_chart(fig)\n",
        "\n",
        "\n",
        "# GRAFICA DE BURBUJAS CONSUMO, EMISIONES Y POBLACION\n",
        "\n",
        "fig = px.scatter(df, x='Emisiones CO2', y='Consumo Energía', animation_frame='Año', animation_group='País',\n",
        "           size='Población', color='País', hover_name='País',\n",
        "           log_x=True, size_max=55, range_x=[10,30000], range_y=[-40,250], labels={\n",
        "                     \"Emisiones CO2\": \"Emisiones CO2 (Mt)\",\n",
        "                     \"Consumo Energía\": \"Consumo Energía (EJ)\",\n",
        "                 },title=\"Emisiones CO2 VS Consumo de Energía\")\n",
        "fig.update_layout(width=800, height=500, title_text = 'Emisiones CO2 VS Consumo de Energía',font_size=14)\n",
        "st.plotly_chart(fig)\n",
        "\n",
        "\n",
        "# GRAFICA DE BARRAS PAISES QUE MAS CONTAMINAN\n",
        "\n",
        "df1 = df.groupby(by=['País'], as_index=False).sum().sort_values('Emisiones CO2', ascending=False).drop(columns='Año').reset_index()\n",
        "df1 = df1.loc[(df1['País'] != 'Other South America (BP)') & (df1['País'] != 'Other CIS (BP)'), :]\n",
        "max_contaminantes = df1.head(10)\n",
        "min_contaminantes = df1.tail(10)\n",
        "\n",
        "graf_peores_pc=px.bar( data_frame=max_contaminantes,\n",
        "                    x=\"País\", \n",
        "                    y=\"Emisiones CO2\", \n",
        "                    title=\"Países con mayor cantidad acumulada de Emisiones de CO2\",\n",
        "                    color_discrete_sequence=[\"red\"],\n",
        "                    opacity=0.8\n",
        "        )\n",
        "st.plotly_chart(graf_peores_pc)\n",
        "\n",
        "  \n",
        "\n",
        "\n",
        "# GRAFICA DE BARRAS PAISES QUE MENOS CONTAMINAN\n",
        "\n",
        "graf_mejores_pc=px.bar( data_frame=min_contaminantes,\n",
        "                    x=\"País\", \n",
        "                    y=\"Emisiones CO2\", \n",
        "                    title=\"Países con menor cantidad acumulada de Emisiones de CO2\",\n",
        "                    color_discrete_sequence=[\"green\"],\n",
        "                    opacity=0.8\n",
        "        )\n",
        "st.plotly_chart(graf_mejores_pc)\n",
        "\n",
        "# Kari  # Kari # Kari  # Kari # Kari  # Kari # Kari  # Kari # Kari  # Kari # Kari  # Kari # Kari  # Kari # Kari  # Kari\n",
        "#GRAFICA QUE MUESTRA INCREMENTO DE EMISIONES CO2 ANUALES EN PORCENTAJE\n",
        "emisionesk = pd.read_sql_query('SELECT *FROM \"Emisiones_CO2\";', con=conexion)\n",
        "paisesk = pd.read_sql_query('SELECT *FROM \"Paises\";', con=conexion)\n",
        "añosk = pd.read_sql_query('SELECT *FROM \"Años\";', con=conexion)\n",
        "emisiones80k=pd.merge(emisionesk,paisesk,on='Id_Pais')\n",
        "emisiones80k=pd.merge(emisiones80k,añosk,on='Id_anio')\n",
        "emisiones80k.drop(columns={'Id_Pais','Id_anio'}, inplace=True)\n",
        "emisiones_añok=emisiones80k.groupby(by=['Anio']).mean()\n",
        "emisiones_añok['Variacion']=(emisiones_añok['Emisiones_CO2'].pct_change())*100\n",
        "k_fig = go.Figure()\n",
        "k_fig.add_trace(go.Scatter(x=emisiones_añok.index, y=emisiones_añok['Variacion'],\n",
        "                    mode='lines'\n",
        "                     ))\n",
        "k_fig.update_layout(title='Porcentaje de incremento de Emisiones CO2 en el mundo por año de  1990 a 2021',\n",
        "                   xaxis_title='Año',\n",
        "                   yaxis_title='Porcentaje de incremento')\n",
        "st.plotly_chart(k_fig, use_container_width=True)\n",
        "\n",
        "\n",
        "\n",
        "\n",
        "# Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza # Gonza\n",
        "query = 'SELECT an.\"Anio\", pa.\"Pais\", (enp.\"Consumo_Petroleo\" + enc.\"Consumo_Carbon\" + eng.\"Consumo_gas\") as \"Consumo total combustible fosiles\" FROM \"Energia_petroleo\" enp JOIN \"Energia_Carbon\" enc ON enp.\"Id_Pais\" = enc.\"Id_Pais\" AND enp.\"Id_anio\" = enc.\"Id_Anio\" JOIN \"Gas_Natural\" eng ON enp.\"Id_Pais\" = eng.\"Id_Pais\" AND enp.\"Id_anio\" = eng.\"Id_anio\" JOIN \"Paises\" pa ON enp.\"Id_Pais\" = pa.\"Id_Pais\" JOIN \"Años\" an ON enp.\"Id_anio\"=an.\"Id_anio\";'\n",
        "conexion = create_engine(\"postgresql://{user}:{pw}@{host}:{port}/{db}\"\n",
        "                         .format(user=\"postgres\",\n",
        "                                 pw=\"postgres\",\n",
        "                                 host=\"database-grupo5.cgmzd7suyc4v.us-east-1.rds.amazonaws.com\",\n",
        "                                 port=\"5432\",\n",
        "                                 db=\"postgres\",\n",
        "                                 echo=False))\n",
        "\n",
        "df_gonza = pd.read_sql_query(query, con=conexion)\n",
        "\n",
        "df_gonza['Consumo total combustible fosiles'] = round(df_gonza['Consumo total combustible fosiles'], 2)\n",
        "\n",
        "df_2020_gonza = df_gonza[df_gonza['Anio'] == 2020]\n",
        "df_2020_gonza = df_2020_gonza.sort_values(by='Consumo total combustible fosiles', ascending=False)\n",
        "\n",
        "fig_gonza1 = px.bar(df_2020_gonza.head(7), y='Consumo total combustible fosiles',x='Pais', text_auto='.2s', \n",
        "                title='Consumo de combustibles fosiles en el año 2020')\n",
        "\n",
        "st.plotly_chart(fig_gonza1)\n",
        "\n",
        "fig_gonza2 = px.bar(df_gonza[df_gonza['Consumo total combustible fosiles']> 13], x='Anio', y='Consumo total combustible fosiles',\n",
        "            color='Pais', text='Pais', title='Evolucion a traves del tiempo')\n",
        "st.plotly_chart(fig_gonza2)\n",
        "\n",
        "df_mayores_gonza = df_gonza.groupby('Pais')['Consumo total combustible fosiles'].sum()\n",
        "df_mayores_gonza = df_mayores_gonza.sort_values(ascending=False)\n",
        "fig_gonza3 = px.bar(df_mayores_gonza.head(7), text_auto='.2s', \n",
        "                title='Consumo de combustibles fosiles total por pais. (1985-2020)')\n",
        "st.plotly_chart(fig_gonza3)\n",
        "\n",
        "\n",
        "\n",
        " # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer  # Fer \n",
        "\n",
        "Energias_Renovables_Fer = Energias_Renovables_Fer.sort_values(by='Anio', ascending=True)\n",
        "min_value = Energias_Renovables_Fer['Total_Produccion'].min()\n",
        "max_value = Energias_Renovables_Fer['Total_Produccion'].max()\n",
        "fig3 = px.choropleth(Energias_Renovables_Fer, locations=\"Codigo_pais\",\n",
        "                    color=\"Total_Produccion\",\n",
        "                    animation_frame='Anio', labels={\n",
        "                     \"Anio\":\"Año\",\"Total_Produccion\":\"Produccion Total\"},\n",
        "                    color_continuous_midpoint = 3,\n",
        "color_continuous_scale=px.colors.sequential.thermal_r,\n",
        "range_color=(min_value,max_value))\n",
        "fig3.update_layout(width=800, height=500, title_text = 'Produccion Total de Energías Renovables (TWh)', font_size=14)\n",
        "st.plotly_chart(fig3)\n",
        "\n",
        "\n",
        "df1 = Energias_Renovables_Fer[Energias_Renovables_Fer['Total_Produccion']>=201.1]\n",
        "df1 = df1.sort_values(by=\"Total_Produccion\", ascending=False)\n",
        "fig1=px.bar(df1, x='Anio', y='Total_Produccion', color='Pais',labels={\n",
        "                     \"Pais\": \"País\"})\n",
        "fig1.update_layout( xaxis_title='Año',\n",
        "                   yaxis_title='Producción Total (TWh)', \n",
        "                   title_text = 'Paises con Mayor Producción de Energías Renovables',\n",
        "                   font_size=14)\n",
        "st.plotly_chart(fig1, use_container_width=True)\n",
        "\n",
        "df3 = er_fer[er_fer['Total_Produccion']<=10]\n",
        "df3 = df3.sort_values(by=\"Total_Produccion\", ascending=False)\n",
        "fig4=px.bar(df3, x='Anio', y='Total_Produccion', color='Pais',labels={\n",
        "                     \"Pais\": \"País\"})\n",
        "fig4.update_layout( xaxis_title='Año',\n",
        "                   yaxis_title='Producción Total (TWh)', \n",
        "                   title_text = 'Paises con Menor Producción de Energías Renovables',\n",
        "                   font_size=14)\n",
        "st.plotly_chart(fig4, use_container_width=True)\n",
        "\n",
        "\n",
        "\n",
        "\n",
        " # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco # Franco\n",
        "\n",
        "\n",
        "queryFranco='SELECT a.\"Anio\", pa.\"Pais\", pa.\"Codigo_pais\", (er.\"Consumo_renovables\"+en.\"Consumo_Nuclear\")/et.\"Consumo_total\" as \"Energia_Limpia\" FROM \"Energia_renovables\" er JOIN \"Energia_nuclear_con_ceros\" en ON  er.\"Id_Pais\" = en.\"Id_Pais\" AND er.\"Id_anio\" = en.\"Id_anio\" JOIN \"Energia_total\" et ON er.\"Id_Pais\" = et.\"Id_Pais\" AND er.\"Id_anio\" = et.\"Id_anio\" JOIN \"Paises\" pa ON er.\"Id_Pais\" = pa.\"Id_Pais\" JOIN \"Años\" a ON er.\"Id_anio\"=a.\"Id_anio\";'\n",
        "FR_Energias_limpias = pd.read_sql_query(queryFranco, con=conexion)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other CIS (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other Northern Africa (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other Western Africa (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other Asia Pacific (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other South America (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Eastern Africa (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other Caribbean (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other Europe (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other Middle East (BP)\"].index, inplace=True)\n",
        "FR_Energias_limpias.drop(FR_Energias_limpias[FR_Energias_limpias.Pais == \"Other South and Central America (BP)\"].index, inplace=True)\n",
        "\n",
        "FR_Energias_limpias2021 = FR_Energias_limpias[FR_Energias_limpias.Anio == 2021]\n",
        "FR_Energias_limpias2021.sort_values(\"Energia_Limpia\", ascending=True, inplace=True)\n",
        "FR_Energias_limpias2021.reset_index(drop=True, inplace=True)\n",
        "\n",
        "#Creacion de mapa de consumo de energías limpias consumo total\n",
        "\n",
        "data1=dict(type = \"choropleth\", \n",
        " locations = FR_Energias_limpias2021.Codigo_pais, \n",
        " locationmode = \"ISO-3\", \n",
        " z = FR_Energias_limpias2021.Energia_Limpia)\n",
        "\n",
        "layout1 = dict(title = 'Consumo energía limpia / consumo total en 2021', title_x=0.5, geo = {'scope':'world'})\n",
        "FR_x1 = go.Figure(data=[data1],\n",
        "layout = layout1)\n",
        "st.write(FR_x1)\n",
        "\n",
        "#gráficos de barra\n",
        "\n",
        "worst_5_energiaslimpias = FR_Energias_limpias2021[0:5]\n",
        "best_5_energiaslimpias = FR_Energias_limpias2021[-5:]\n",
        "min_medio=int(FR_Energias_limpias2021.shape[0]/2-2)\n",
        "max_medio=int(FR_Energias_limpias2021.shape[0]/2+3)\n",
        "middle_5_energiaslimpias=FR_Energias_limpias2021[min_medio:max_medio]\n",
        "\n",
        "#Peores, gráfico de barras.\n",
        "\n",
        "graf_peores_energialimpia=px.bar( data_frame=worst_5_energiaslimpias,\n",
        "                    x=\"Pais\", \n",
        "                    y=\"Energia_Limpia\", \n",
        "                    title=\"5 países con peor relación Consumo energía limpia / consumo total en 2021\",\n",
        "                    color_discrete_sequence=[\"red\"],\n",
        "                    opacity=0.8\n",
        "        )\n",
        "st.write(graf_peores_energialimpia)\n",
        "\n",
        "#intermedios, gráfico de barras.\n",
        "\n",
        "graf_middle_energialimpia=px.bar( data_frame=middle_5_energiaslimpias,\n",
        "                    x=\"Pais\", \n",
        "                    y=\"Energia_Limpia\", \n",
        "                    title=\"5 países intermedios según relación Consumo energía limpia / consumo total en 2021\",\n",
        "                    color_discrete_sequence=[\"blue\"],\n",
        "                    opacity=0.8\n",
        "        )\n",
        "st.write(graf_middle_energialimpia)\n",
        "\n",
        "#Mejores, gráfico de barras.\n",
        "\n",
        "graf_mejores_energialimpia=px.bar( data_frame=best_5_energiaslimpias,\n",
        "                    x=\"Pais\", \n",
        "                    y=\"Energia_Limpia\", \n",
        "                    title=\"5 países con mejor relación Consumo energía limpia / consumo total en 2021\",\n",
        "                    color_discrete_sequence=[\"green\"],\n",
        "                    opacity=0.8\n",
        "        )\n",
        "st.write(graf_mejores_energialimpia)\n",
        "\n",
        "KPI5=FR_Energias_limpias.pivot(index=\"Pais\",columns=\"Anio\", values=\"Energia_Limpia\")\n",
        "KPI5[\"KPI5\"]=KPI5[2021]-KPI5[2020]\n",
        "tablakpi5=KPI5[KPI5[\"KPI5\"]>=0.01][\"KPI5\"].to_frame()\n",
        "tablakpi5[\"KPI5\"]=tablakpi5[\"KPI5\"]*100\n",
        "tablakpi5[\"KPI5\"]=round(tablakpi5[\"KPI5\"],1)\n",
        "tablakpi5=tablakpi5.sort_values(\"KPI5\", ascending=False)\n",
        "st.write(px.bar(tablakpi5, title=\"Incremento interanual del ratio Consumo Limpio / Consumo Total (2021)\", labels={'x': 'Year', 'value':'Puntos porcentuales'}))\n",
        "\n",
        "\n",
        "paiseskpi5=list(KPI5.index.values)\n",
        "from sklearn.model_selection import train_test_split\n",
        "from sklearn.linear_model import LinearRegression\n",
        "from sklearn.metrics import mean_absolute_error, mean_squared_error\n",
        "import numpy as np\n",
        "alfas = {}\n",
        "betas = {}\n",
        "score= {}\n",
        "\n",
        "for pais in paiseskpi5:\n",
        "    seriepais=KPI5.loc[pais]\n",
        "    seriepais=seriepais.loc[1995:2021]\n",
        "    seriepais=pd.DataFrame(seriepais)\n",
        "    seriepais[\"X\"]=range(1, len(seriepais)+1)\n",
        "    reg=LinearRegression()\n",
        "    X = seriepais.X.values.reshape(-1,1)\n",
        "    y = seriepais[pais].values.reshape(-1,1)\n",
        "    reg.fit(X=X, y=y)\n",
        "    alfas[pais] = reg.coef_[0][0]\n",
        "    betas[pais] = reg.intercept_[0]\n",
        "    score[pais] =reg.score(X,y)\n",
        "\n",
        "#Cambio para Japon por el accidente de 2011\n",
        "\n",
        "seriepais=KPI5.loc[\"Japan\"]\n",
        "seriepais=seriepais.loc[2012:2021]\n",
        "seriepais=pd.DataFrame(seriepais)\n",
        "seriepais[\"X\"]=range(1, len(seriepais)+1)\n",
        "reg=LinearRegression()\n",
        "X = seriepais.X.values.reshape(-1,1)\n",
        "y = seriepais[\"Japan\"].values.reshape(-1,1)\n",
        "reg.fit(X=X, y=y)\n",
        "alfas[\"Japan\"] = reg.coef_[0][0]\n",
        "betas[\"Japan\"] = reg.intercept_[0]\n",
        "score[\"Japan\"] =reg.score(X,y)\n",
        "\n",
        "dfalfas=pd.DataFrame(alfas, index=alfas.keys())\n",
        "\n",
        "pendientes=pd.DataFrame(dfalfas.iloc[0])\n",
        "pendientes.rename(columns={\"Argentina\":\"Tendencia\"}, inplace=True)\n",
        "pendientes[\"Tendencia\"] = pendientes[\"Tendencia\"]*100\n",
        "pendientes.sort_values(\"Tendencia\", ascending=False, inplace=True)\n",
        "pendientes[\"Tendencia\"]=round(pendientes[\"Tendencia\"],2)\n",
        "best_5_pendientes = pendientes[0:5]\n",
        "worst_5_pendientes = pendientes[-5:]\n",
        "pendientes_min_medio=int(pendientes.shape[0]/2-2)\n",
        "pendientes_max_medio=int(pendientes.shape[0]/2+3)\n",
        "middle_5_pendientes=pendientes[pendientes_min_medio:pendientes_max_medio]\n",
        "#Peores, gráfico de barras.\n",
        "\n",
        "graf_peores_pendientes=px.bar( data_frame=worst_5_pendientes,\n",
        "                    x=worst_5_pendientes.index, \n",
        "                    y=\"Tendencia\", \n",
        "                    title=\"5 países con menor pendiente en su tendencia lineal\",\n",
        "                    color_discrete_sequence=[\"red\"],\n",
        "                    opacity=0.8,\n",
        "                    labels={'index': 'País', \"Tendencia\": \"Pendiente de la tendencia\"}\n",
        "        )\n",
        "st.write(graf_peores_pendientes)\n",
        "\n",
        "\n",
        "#intermedios, gráfico de barras.\n",
        "\n",
        "graf_middle_pendientes=px.bar( data_frame=middle_5_pendientes,\n",
        "                    x=middle_5_pendientes.index, \n",
        "                    y=\"Tendencia\", \n",
        "                    title=\"5 países intermedios según la pendiente de su línea de tendencia\",\n",
        "                    color_discrete_sequence=[\"blue\"],\n",
        "                    opacity=0.8,\n",
        "                    labels={'index': 'País', \"Tendencia\": \"Pendiente de la tendencia\"}\n",
        "        )\n",
        "st.write(graf_middle_pendientes)\n",
        "\n",
        "\n",
        "#Mejores, gráfico de barras.\n",
        "\n",
        "graf_mejores_pendientes=px.bar( data_frame=best_5_pendientes,\n",
        "                    x=best_5_pendientes.index, \n",
        "                    y=\"Tendencia\", \n",
        "                    title=\"5 países con mayor pendiente en su tendencia lineal\",\n",
        "                    color_discrete_sequence=[\"green\"],\n",
        "                    opacity=0.8,\n",
        "                    labels={'index': 'País', \"Tendencia\": \"Pendiente de la tendencia\"}\n",
        "        )\n",
        "st.write(graf_mejores_pendientes)\n",
        "\n",
        "predicciones=pendientes.merge(FR_Energias_limpias2021,left_index=True, right_on=\"Pais\")\n",
        "predicciones.drop(\"Anio\", axis=1, inplace=True)\n",
        "predicciones.rename(columns={\"Energia_Limpia\":\"2021\"}, inplace=True)\n",
        "listaaños=list(np.arange(2022,2051))\n",
        "for año in listaaños:\n",
        "    predicciones[año]=predicciones[\"2021\"]+(predicciones.Tendencia/100)*(año-2021)\n",
        "    predicciones.loc[ predicciones[año] >1, año] = 1\n",
        "    predicciones.loc[ predicciones[año] <0, año] = 0\n",
        "\n",
        "predicciones.set_index(\"Pais\", inplace=True)\n",
        "\n",
        "#Creacion de mapa de consumo de energías limpias consumo total\n",
        "\n",
        "data2=dict(type = \"choropleth\", \n",
        " locations = predicciones.Codigo_pais, \n",
        " locationmode = \"ISO-3\", \n",
        " z = predicciones[2050])\n",
        "\n",
        "layout2 = dict(title = 'Consumo energía limpia / Consumo total en 2050', title_x=0.5, geo = {'scope':'world'})\n",
        "FR_x2 = go.Figure(data=[data2],\n",
        "layout = layout2)\n",
        "st.write(FR_x2)\n",
        "\n",
        "#GRAFICO IRRADIANZA VS CAPACIDAD INSTALADA solar\n",
        "\n",
        "query1sol= 'SELECT p.\"Pais\", i.\"Irradianza\" FROM \"Irradianza\" i JOIN \"Paises\" p ON (i.\"Id_Pais\" = p.\"Id_Pais\") WHERE i.\"Id_Anio\"=56;'\n",
        "irradianza=pd.read_sql_query(query1sol, con=conexion)\n",
        "query2sol='SELECT p.\"Pais\", es.\"Instalada_solar\" FROM \"Energia_solar\" es JOIN \"Paises\" p ON (es.\"Id_Pais\" = p.\"Id_Pais\") WHERE es.\"Id_anio\"=56;'\n",
        "FR_solar=pd.read_sql_query(query2sol, con=conexion)\n",
        "datossolar=pd.merge(irradianza, FR_solar, on=\"Pais\")\n",
        "st.write(px.scatter(data_frame=datossolar, \n",
        "           x=\"Instalada_solar\", \n",
        "           y=\"Irradianza\", \n",
        "           hover_data=[\"Pais\"], \n",
        "           title=\"Energía solar: irradianza vs capacidad instalada (2021)\", \n",
        "           labels={'Irradianza': 'Irradianza (W/m2)', \"Instalada_solar\": \"Instalada_solar (MW)\"},\n",
        "           color=\"Pais\"))\n",
        "\n",
        "\n",
        "\n",
        "# Machine Learning # Machine Learning # Machine Learning # Machine Learning # Machine Learning\n",
        "\n",
        "import streamlit as st\n",
        "import plotly.express as px\n",
        "import matplotlib.pyplot as plt\n",
        "from sqlalchemy import create_engine\n",
        "import pandas as pd\n",
        "import numpy as np\n",
        "import seaborn as sns\n",
        "from sklearn.linear_model import LinearRegression\n",
        "from sklearn import linear_model\n",
        "from sklearn.model_selection import train_test_split\n",
        "from sklearn import metrics\n",
        "import datetime\n",
        "\n",
        "def plot_corre_heatmap(corr):\n",
        "    '''\n",
        "    Definimos una función para ayudarnos a graficar un heatmap de correlación\n",
        "    '''\n",
        "    fig = plt.figure(figsize=(12,10))\n",
        "    sns.heatmap(corr, cbar = True,  square = False, annot=True, fmt= '.2f'\n",
        "                ,annot_kws={'size': 15},cmap= 'coolwarm')\n",
        "    plt.xticks(rotation = 45)\n",
        "    plt.yticks(rotation = 45)\n",
        "    # Arreglamos un pequeño problema de visualización\n",
        "    b, t = plt.ylim() # discover the values for bottom and top\n",
        "    b += 0.5 # Add 0.5 to the bottom\n",
        "    t -= 0.5 # Subtract 0.5 from the top\n",
        "    plt.ylim(b, t) # update the ylim(bottom, top) values\n",
        "    st.pyplot(fig)\n",
        "\n",
        "\n",
        "def listar (lista):\n",
        "    return list(map(lambda x: x[0], lista))\n",
        "\n",
        "\n",
        "\n",
        "st.subheader('MODELO ML')\n",
        "st.markdown(\"# Regresión Lineal para Cálculo de Emisiones de CO2 por País\")\n",
        "conexion = create_engine(\"postgresql://{user}:{pw}@{host}:{port}/{db}\".format(\n",
        "                                    user=\"postgres\",\n",
        "                                    pw=\"postgres\",\n",
        "                                    host=\"database-grupo5.cgmzd7suyc4v.us-east-1.rds.amazonaws.com\",\n",
        "                                    port = \"5432\",\n",
        "                                    db = \"postgres\",\n",
        "                                    echo=False))\n",
        "df_paises_j = pd.read_sql_query('SELECT * FROM \"Paises\"',con=conexion)\n",
        "df_Años_j = pd.read_sql_query('SELECT * FROM \"Años\"',con=conexion)\n",
        "df_emisiones_CO2_j = pd.read_sql_query('SELECT * FROM \"Emisiones_CO2\"',con=conexion)\n",
        "df_poblacion_j = pd.read_sql_query('SELECT * FROM \"Poblacion_Mundo\"',con=conexion)\n",
        "df_consumo_j = pd.read_sql_query('SELECT * FROM \"Energia_total\"',con=conexion)\n",
        "df_renobables_j = pd.read_sql_query('SELECT * FROM \"Energia_renovables\"',con=conexion)\n",
        "df_poblacion_j = df_poblacion_j.rename(columns={'Id_Anio':'Id_anio'})\n",
        "df_poblacion_j = df_poblacion_j.rename(columns={'Id_Anio':'Id_anio'})\n",
        "df = df_emisiones_CO2_j.merge(df_poblacion_j, on=['Id_Pais','Id_anio'], how='left').dropna()\n",
        "df = df.merge(df_consumo_j, on=['Id_Pais','Id_anio'], how='left').dropna()\n",
        "df = df.merge(df_renobables_j, on=['Id_Pais','Id_anio'], how='left').dropna()\n",
        "df = df.merge(df_paises_j, on='Id_Pais', how='left')\n",
        "df = df.merge(df_Años_j, on='Id_anio', how='left')\n",
        "df = df.drop(columns=['ID_Emisiones_CO2','Id_Pais','Id_anio','Id_Poblacion','ID_consumototal','ID_Energia_renovables','Codigo_pais','Produccion_renovables'])\n",
        "df = df.reindex(columns=['Pais','Anio','Poblacion','Emisiones_CO2','Consumo_renovables','Consumo_total'])\n",
        "df['Poblacion'] = df['Poblacion'].astype(int)\n",
        "year = datetime.date.today().strftime(\"%Y\")\n",
        "anios = [i for i in range(int(year),2101)]\n",
        "filter_pais = st.selectbox(\"Selecciona un país\", np.sort(pd.unique(df[\"Pais\"])))\n",
        "pais = filter_pais\n",
        "df1 = df[df['Pais']== pais]\n",
        "x = df1['Anio']\n",
        "y1 = df1['Emisiones_CO2']\n",
        "y2 = df1['Poblacion']\n",
        "y3 = df1['Consumo_renovables']\n",
        "y4 = df1['Consumo_total']\n",
        "fig, ax = plt.subplots(4, figsize=(15, 15))\n",
        "ax[0].plot(x, y1)\n",
        "ax[0].set(ylabel='Emisiones (Mt)', title='Emisiones CO2')\n",
        "ax[1].plot(x, y2)\n",
        "ax[1].set(ylabel='Población (M)', title='Población')\n",
        "ax[2].plot(x, y3)\n",
        "ax[2].set(ylabel='Consumo renovables (EJ)', title='Consumo Energías Renovables')\n",
        "ax[3].plot(x, y4)\n",
        "ax[3].set(xlabel= 'Año', ylabel='Consumo total (EJ)', title='Consumo Total')\n",
        "st.pyplot(fig)\n",
        "corr = df1.corr()\n",
        "plot_corre_heatmap(corr)\n",
        "filter_anio = st.selectbox(\"Selecciona un año\", np.sort(pd.unique(anios)))\n",
        "anio = filter_anio\n",
        "X = df1['Anio'].values\n",
        "y = df1['Emisiones_CO2'].values\n",
        "X = X.reshape(-1,1)\n",
        "y = y.reshape(-1,1)\n",
        "model = LinearRegression(fit_intercept=True)\n",
        "model.fit(X, y)\n",
        "X_pred = np.arange(df1.Anio.max() + 1, df1.Anio.max() + (anio - df1.Anio.max() + 1)).reshape(-1,1)\n",
        "y_pred = model.predict(X_pred)\n",
        "max_value = df1[df1.Anio == df1.Anio.max()].Emisiones_CO2.values\n",
        "coef = 0.92 #El KPI requiere que se reduzcan un 8% de las emisiones anuales\n",
        "co2 = [coef*max_value]\n",
        "for i in range(len(y_pred)-1):\n",
        "    co2.append(coef*co2[i-1])\n",
        "x =  listar(X_pred.tolist())\n",
        "y1 = listar(y_pred.tolist())\n",
        "y2 = listar([i.tolist() for i in co2])\n",
        "st.write(f'Las emisiones de {pais} esperadas para el año {x[-1]} son de {int(y1[-1])} Mt de CO2')\n",
        "st.write(f'Las emisiones de {pais} esperadas para el año {x[-1]} son de {int(y2[-1])} Mt de CO2 en caso de reducir un 8% anual')\n",
        "x = ['Predicción','Reduciendo Emisiones']\n",
        "y = [y1[-1], y2[-1]]\n",
        "fig = plt.figure(figsize=(6, 3))\n",
        "ax = fig.add_axes([0,0,1,1])\n",
        "ax.bar(x,y)\n",
        "ax.set_title('Emisiones de CO2 (Mt)')\n",
        "st.write(fig)\n",
        "\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "mAoc_XDq80qN",
        "outputId": "563a5936-1819-4c63-b650-08ccaf22a9bd"
      },
      "execution_count": 30,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Overwriting app.py\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "from pyngrok import ngrok\n",
        "ngrok.set_auth_token('2EOquGz4jjdxVe66ox1L67t91S1_64WVefntJSyEC9deHstKn')"
      ],
      "metadata": {
        "id": "tVQCAuAY9fT3",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "8cd39597-ab9d-47fe-a084-c3a969a38240"
      },
      "execution_count": 4,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": []
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "!nohup streamlit run app.py --server.port 80 &\n",
        "ngrok.connect(port='80')"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "x58hXEmVxjqn",
        "outputId": "0f007f30-a349-465f-c8bd-57ba3d44672e"
      },
      "execution_count": 5,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "nohup: appending output to 'nohup.out'\n"
          ]
        },
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "<NgrokTunnel: \"http://b32a-35-184-255-142.ngrok.io\" -> \"http://localhost:80\">"
            ]
          },
          "metadata": {},
          "execution_count": 5
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "!ngrok http --host-header=rewrite localhost:80"
      ],
      "metadata": {
        "id": "VDJvnu4Bx8XX",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "94f0adfc-86a7-4c8a-fedc-0359e7aff97a"
      },
      "execution_count": 6,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Your account is limited to 1 simultaneous ngrok agent session.\n",
            "Active ngrok agent sessions in region 'us':\n",
            "  - ts_2F3Sx0BH7lNQmRmi1YBnqGWTL8l (35.184.255.142)\r\n",
            "\r\n",
            "ERR_NGROK_108\r\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "tunnels=ngrok.get_tunnels()"
      ],
      "metadata": {
        "id": "LdmElWdOxlj5"
      },
      "execution_count": 7,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "tunnels"
      ],
      "metadata": {
        "id": "5xhMyG_NXA-H",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "3bd32f52-32b0-4a9a-aad0-1bbf3f3240f2"
      },
      "execution_count": 8,
      "outputs": [
        {
          "output_type": "execute_result",
          "data": {
            "text/plain": [
              "[<NgrokTunnel: \"https://b32a-35-184-255-142.ngrok.io\" -> \"http://localhost:80\">,\n",
              " <NgrokTunnel: \"http://b32a-35-184-255-142.ngrok.io\" -> \"http://localhost:80\">]"
            ]
          },
          "metadata": {},
          "execution_count": 8
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "#pip install SQLAlchemy"
      ],
      "metadata": {
        "id": "FoAitEqxdCS0"
      },
      "execution_count": 9,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "import pandas as pd\n",
        "from sqlalchemy import create_engine\n",
        "conexion= create_engine('postgresql://{user}:{pw}@{host}:{port}/{db}'.format(\n",
        "    user='postgres', pw='postgres', host='database-grupo5.cgmzd7suyc4v.us-east-1.rds.amazonaws.com',port='5432',db='postgres', echo=False))\n",
        "\n",
        "if conexion.connect():\n",
        "    print('Conexion Exitosa')\n"
      ],
      "metadata": {
        "id": "AgRaNgNacotO",
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "outputId": "6302b234-d177-428b-d99b-ecbe4fcbf5b7"
      },
      "execution_count": 10,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "Conexion Exitosa\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "paises = pd.read_sql_query('SELECT * FROM \"Paises\"',con=conexion)\n",
        "anos = pd.read_sql_query('SELECT * FROM \"Años\"' ,con=conexion)\n",
        "emisiones_sin_año = pd.read_sql_query ('SELECT * FROM \"Emisiones_CO2\"',con=conexion)\n",
        "nuclear_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_nuclear\"',con=conexion)\n",
        "eolica_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_Eolica\"',con=conexion)\n",
        "geotermica_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_Geotermica\"',con=conexion)\n",
        "solar_sin_año = pd.read_sql_query ('SELECT * FROM \"Energia_solar\"',con=conexion)"
      ],
      "metadata": {
        "id": "8v0bq_KGclL_"
      },
      "execution_count": 11,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#pip install plotly"
      ],
      "metadata": {
        "id": "ZwDkc-PYI3G_"
      },
      "execution_count": 12,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "import plotly.express as px"
      ],
      "metadata": {
        "id": "5wYgZyB8I22c"
      },
      "execution_count": 13,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Cambie el nombre de la columna Años porque en todas las energias estaba diferente\n"
      ],
      "metadata": {
        "id": "uIB4kC49g_qO"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "#anos=anos.rename(columns={'Id_Años':'Id_anio'})\n",
        "#Años=anos\n",
        "#Años.to_csv('Años.csv',index=False)\n",
        "#Años.to_sql(con=conexion,if_exists='replace',index=False,name='Años')"
      ],
      "metadata": {
        "id": "YQgCbFizax4z"
      },
      "execution_count": 14,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#paises=paises.rename(columns={'Id_pais':'Id_Pais'})\n",
        "#Paises=paises\n",
        "#Paises.to_sql(con=conexion,if_exists='replace',index=False,name='Paises')"
      ],
      "metadata": {
        "id": "BzSC85suiR-F"
      },
      "execution_count": 15,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#geotermica_sin_año=geotermica_sin_año.rename(columns={'Id_Anio':'Id_anio'})\n",
        "#geotermica_sin_año.to_sql(con=conexion,if_exists='replace',index=False,name='Energia_Geotermica')\n",
        "#Años=anos\n",
        "#Años.to_csv('Años.csv',index=False)"
      ],
      "metadata": {
        "id": "XUnVLIKoeGeJ"
      },
      "execution_count": 16,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#nuclear=pd.merge(nuclear_sin_año,anos)\n",
        "#nuclear=pd.merge(nuclear,paises)\n",
        "#nuclear.head(2)\n",
        "#solar=pd.merge(solar_sin_año,anos)\n",
        "#solar=pd.merge(solar,paises)\n",
        "#eolica=pd.merge(eolica_sin_año,anos)\n",
        "#eolica=pd.merge(eolica,paises)\n",
        "#geotermica_sin_año=geotermica_sin_año.rename(columns={'Id_Anio':'Id_anio'})\n",
        "#geotermica_sin_año[['Id_Anio']]=geotermica_sin_año[['Id_Anio']].astype(int)\n",
        "#geotermica=pd.merge(geotermica_sin_año,anos)\n",
        "#geotermica=pd.merge(geotermica_sin_año,paises)\n",
        "#combinados1=pd.merge(nuclear, geotermica)\n",
        "#combinados1=pd.merge(combinados1,solar)\n",
        "#Energias_Renovables=pd.merge(combinados1,eolica)\n",
        "#Energias_Renovables['Consumo_Eolica']=Energias_Renovables['Consumo_Eolica'].astype(float)\n",
        "#Energias_Renovables['Produccion_Eolica']=Energias_Renovables['Produccion_Eolica'].astype(float)\n",
        "#Energias_Renovables['Produccion_Geotermica']=Energias_Renovables['Produccion_Geotermica'].astype(float)\n",
        "#Energias_Renovables['Total_Consumo']=Energias_Renovables['Consumo_Nuclear']+Energias_Renovables['Consumo_solar']+Energias_Renovables['Consumo_Eolica']\n",
        "#Energias_Renovables['Total_Produccion']=Energias_Renovables['Produccion_Nuclear']+Energias_Renovables['Produccion_solar']+Energias_Renovables['Produccion_Eolica']"
      ],
      "metadata": {
        "id": "WcM2exBEB0zS"
      },
      "execution_count": 17,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Continuamos para construir las graficas"
      ],
      "metadata": {
        "id": "wtqPwSb0aiOP"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "Energias_Renovables_Fer = pd.read_sql_query ('SELECT * FROM \"Energias_Renovables_Fer\"',con=conexion)"
      ],
      "metadata": {
        "id": "6D4O8fNnCqXU"
      },
      "execution_count": 18,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "fig = px.line(Energias_Renovables_Fer, x=\"Anio\", y=\"Produccion_Nuclear\", color=\"Pais\")\n",
        "fig.update_layout(title_text = 'Producción de Energía Nuclear (1995-2021)')\n",
        "fig.show()"
      ],
      "metadata": {
        "id": "YNTSlLdtgXQu",
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 542
        },
        "outputId": "41432d59-fe08-49b2-c4e5-7b1f026fe1e2"
      },
      "execution_count": 19,
      "outputs": [
        {
          "output_type": "display_data",
          "data": {
            "text/html": [
              "<html>\n",
              "<head><meta charset=\"utf-8\" /></head>\n",
              "<body>\n",
              "    <div>            <script src=\"https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.5/MathJax.js?config=TeX-AMS-MML_SVG\"></script><script type=\"text/javascript\">if (window.MathJax) {MathJax.Hub.Config({SVG: {font: \"STIX-Web\"}});}</script>                <script type=\"text/javascript\">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>\n",
              "        <script src=\"https://cdn.plot.ly/plotly-2.8.3.min.js\"></script>                <div id=\"74141ec6-ef77-449c-bb4f-793923126ac8\" class=\"plotly-graph-div\" style=\"height:525px; width:100%;\"></div>            <script type=\"text/javascript\">                                    window.PLOTLYENV=window.PLOTLYENV || {};                                    if (document.getElementById(\"74141ec6-ef77-449c-bb4f-793923126ac8\")) {                    Plotly.newPlot(                        \"74141ec6-ef77-449c-bb4f-793923126ac8\",                        [{\"hovertemplate\":\"Pais=Canada<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Canada\",\"line\":{\"color\":\"#636efa\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Canada\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[92.1,82.0,71.0,73.0,72.3,76.2,75.0,74.4,89.8,91.4,97.3,92.8,95.4,89.5,90.0,92.9,94.2,102.7,106.5,101.1,100.7,100.6,100.0,100.5,97.5,92.0],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Mexico<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Mexico\",\"line\":{\"color\":\"#EF553B\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Mexico\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[10.5,9.3,10.0,8.2,8.7,9.7,10.5,9.2,10.8,10.9,10.4,9.8,10.5,5.9,10.1,8.8,11.8,9.7,11.6,10.6,10.9,13.6,11.2,11.2,11.9],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=United States<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"United States\",\"line\":{\"color\":\"#00cc96\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"United States\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[708.8,710.2,661.7,709.2,766.6,793.6,809.3,821.1,803.9,830.0,823.1,828.7,848.9,848.6,840.9,849.4,831.8,809.8,830.5,839.1,839.1,848.1,847.3,849.6,852.0,831.5,819.1],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Argentina<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Argentina\",\"line\":{\"color\":\"#ab63fa\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Argentina\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[6.4,6.4,6.2,5.5,7.0,8.3,6.1,6.9,8.4,10.7,10.8],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Brazil<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Brazil\",\"line\":{\"color\":\"#FFA15A\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Brazil\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[13.4,11.6,9.9,13.8,12.3,14.0,13.0,14.5,15.7,16.0,15.4,15.4,14.7,15.9,15.7,15.7,16.1,14.1,14.7],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Belgium<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Belgium\",\"line\":{\"color\":\"#19d3f3\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Belgium\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[47.4,47.4,47.3,47.6,46.6,48.2,45.6,47.2,47.9,48.2,40.3,42.6,33.7,26.1,43.5,42.2,28.6,43.5,34.4,50.6],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Bulgaria<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Bulgaria\",\"line\":{\"color\":\"#FF6692\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Bulgaria\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[15.3,15.2,16.3,15.8,14.2,15.9,15.4,15.8,15.5,16.1,16.6,16.6,16.5],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Czechia<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Czechia\",\"line\":{\"color\":\"#B6E880\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Czechia\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[26.2,26.6,27.2,28.0,28.3,30.3,30.7,30.3,26.8,24.1,28.3,29.9,30.2,30.0,30.7],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Finland<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Finland\",\"line\":{\"color\":\"#FF97FF\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Finland\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[23.0,24.1,23.5,23.8],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=France<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"France\",\"line\":{\"color\":\"#FECB52\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"France\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[395.5,388.0,394.2,415.2,421.1,436.8,441.1,448.2,451.5,450.2,439.7,439.4,409.7,428.5,442.4,425.4,423.7,436.5,437.4,403.2,398.4,412.9,399.0,353.8,379.4],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Germany<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Germany\",\"line\":{\"color\":\"#636efa\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Germany\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[161.6,170.3,161.6,170.0,169.6,171.3,164.8,165.1,167.1,163.0,167.4,140.5,148.8,134.9,140.6,108.0,99.5,97.3,97.1,91.8,84.6,76.3,76.0,75.1,64.4,69.0],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Hungary<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Hungary\",\"line\":{\"color\":\"#EF553B\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Hungary\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[14.8,15.4,15.8,15.7,15.8,15.4,15.6,15.8,16.1,16.1,15.7,16.3,16.1,16.0],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Netherlands<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Netherlands\",\"line\":{\"color\":\"#00cc96\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Netherlands\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[4.2,2.4,3.8,3.8,3.9,4.0,3.9,4.0,3.8,4.0,3.5,4.2,4.2,4.2,4.0,4.1,3.9,2.9,4.1,4.1,4.0,3.4,3.5,3.9,4.1,3.8],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Spain<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Spain\",\"line\":{\"color\":\"#ab63fa\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Spain\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[56.3,55.3,59.0,58.9,62.2,63.7,63.0,61.9,63.6,57.5,60.1,55.1,59.0,52.8,61.6,57.7,61.5,56.7,57.3,57.3,58.6,58.1,55.8,58.3,58.3,56.5],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Sweden<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Sweden\",\"line\":{\"color\":\"#FFA15A\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Sweden\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[74.3,69.9,73.6,73.2,57.3,72.1,68.1,67.4,77.7,72.7,67.0,67.0,63.9,52.2,57.7,60.5,64.0,66.5,64.9,56.3,63.1,65.7,68.5,66.1,49.2,53.1],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Switzerland<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Switzerland\",\"line\":{\"color\":\"#19d3f3\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Switzerland\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[26.9,25.6,26.2,27.8,23.3,21.3,20.5,24.4,25.3,23.0,18.5],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Ukraine<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Ukraine\",\"line\":{\"color\":\"#FF6692\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Ukraine\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[89.2,90.2,90.1,83.2,88.4,87.6,81.0,85.6,84.4,83.0,76.2,86.2],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=United Kingdom<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"United Kingdom\",\"line\":{\"color\":\"#B6E880\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"United Kingdom\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[95.1,85.1,90.1,87.8,88.7,80.0,81.6,75.5,63.0,52.5,69.1,62.1,69.0,70.4,70.6,63.7,70.3,71.7,70.3,65.1,56.2,50.3,45.9],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Russia<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Russia\",\"line\":{\"color\":\"#FF97FF\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Russia\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[172.5,180.8,195.5,196.6,203.1,204.6,209.0,215.9,222.4],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Other CIS (BP)<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Other CIS (BP)\",\"line\":{\"color\":\"#FECB52\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Other CIS (BP)\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2007,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[2.6,2.5,2.5,2.3,2.4,2.5,2.8,2.4,2.6,2.1,2.2,2.8,2.0],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=South Africa<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"South Africa\",\"line\":{\"color\":\"#636efa\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"South Africa\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[13.5,12.9,13.0,14.1,13.8,12.2,15.0,14.2,11.6,13.6,13.9,10.4],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=China<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"China\",\"line\":{\"color\":\"#EF553B\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"China\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[14.3,14.4,14.1,14.9,16.7,17.5,25.1,43.3,50.5,53.1,54.8,62.1,68.4,70.1,74.7,87.2,98.3,111.5,133.2,171.4,213.2,248.1,295.0,348.7,366.2,407.5],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=India<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"India\",\"line\":{\"color\":\"#00cc96\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"India\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[15.8,18.9,19.4,18.1,21.3,17.7,17.6,17.8,15.2,16.8,23.1,32.2,33.1,33.3,34.7,38.3,37.9,37.4,39.1,45.2,44.6,43.9],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Japan<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Japan\",\"line\":{\"color\":\"#ab63fa\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Japan\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[321.2,326.0,317.2,319.1,320.5,314.3,230.1,285.9,293.0,304.3,279.0,251.7,274.7,292.4,162.9,18.0,14.6,4.5,17.7,29.1,49.1,65.6,43.0,61.2],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Pakistan<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Pakistan\",\"line\":{\"color\":\"#FFA15A\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Pakistan\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[1.7,2.5,2.5,3.9,2.7,4.3,4.7,4.3,5.3,8.1,9.2,9.2,9.5,15.9],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=South Korea<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"South Korea\",\"line\":{\"color\":\"#19d3f3\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"South Korea\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[109.0,112.1,119.1,129.7,130.7,146.8,148.7,142.9,151.0,147.8,148.6,154.7,150.3,138.8,156.4,164.8,162.0,148.4,133.5,145.9,160.2,158.0],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Taiwan<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Taiwan\",\"line\":{\"color\":\"#FF6692\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Taiwan\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[39.5,40.0,39.9,40.5,40.8,41.6,41.6,42.1,40.4,41.6,42.4,36.5,31.7,22.4,27.7,32.3,31.4,27.8],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Romania<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Romania\",\"line\":{\"color\":\"#B6E880\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Romania\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[11.7,11.5,11.6,11.7,11.6,11.3,11.5,11.4,11.3,11.5,11.3],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Iran<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Iran\",\"line\":{\"color\":\"#FF97FF\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Iran\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2017,2018,2019,2020,2021],\"xaxis\":\"x\",\"y\":[7.0,6.9,6.4,6.3,3.5],\"yaxis\":\"y\",\"type\":\"scatter\"},{\"hovertemplate\":\"Pais=Belarus<br>Anio=%{x}<br>Produccion_Nuclear=%{y}<extra></extra>\",\"legendgroup\":\"Belarus\",\"line\":{\"color\":\"#FECB52\",\"dash\":\"solid\"},\"marker\":{\"symbol\":\"circle\"},\"mode\":\"lines\",\"name\":\"Belarus\",\"orientation\":\"v\",\"showlegend\":true,\"x\":[2020,2021],\"xaxis\":\"x\",\"y\":[0.3,5.8],\"yaxis\":\"y\",\"type\":\"scatter\"}],                        {\"template\":{\"data\":{\"bar\":[{\"error_x\":{\"color\":\"#2a3f5f\"},\"error_y\":{\"color\":\"#2a3f5f\"},\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"bar\"}],\"barpolar\":[{\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"barpolar\"}],\"carpet\":[{\"aaxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"baxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"type\":\"carpet\"}],\"choropleth\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"choropleth\"}],\"contour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"contour\"}],\"contourcarpet\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"contourcarpet\"}],\"heatmap\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmap\"}],\"heatmapgl\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmapgl\"}],\"histogram\":[{\"marker\":{\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"histogram\"}],\"histogram2d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2d\"}],\"histogram2dcontour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2dcontour\"}],\"mesh3d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"mesh3d\"}],\"parcoords\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"parcoords\"}],\"pie\":[{\"automargin\":true,\"type\":\"pie\"}],\"scatter\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter\"}],\"scatter3d\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter3d\"}],\"scattercarpet\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattercarpet\"}],\"scattergeo\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergeo\"}],\"scattergl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergl\"}],\"scattermapbox\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattermapbox\"}],\"scatterpolar\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolar\"}],\"scatterpolargl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolargl\"}],\"scatterternary\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterternary\"}],\"surface\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"surface\"}],\"table\":[{\"cells\":{\"fill\":{\"color\":\"#EBF0F8\"},\"line\":{\"color\":\"white\"}},\"header\":{\"fill\":{\"color\":\"#C8D4E3\"},\"line\":{\"color\":\"white\"}},\"type\":\"table\"}]},\"layout\":{\"annotationdefaults\":{\"arrowcolor\":\"#2a3f5f\",\"arrowhead\":0,\"arrowwidth\":1},\"autotypenumbers\":\"strict\",\"coloraxis\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"colorscale\":{\"diverging\":[[0,\"#8e0152\"],[0.1,\"#c51b7d\"],[0.2,\"#de77ae\"],[0.3,\"#f1b6da\"],[0.4,\"#fde0ef\"],[0.5,\"#f7f7f7\"],[0.6,\"#e6f5d0\"],[0.7,\"#b8e186\"],[0.8,\"#7fbc41\"],[0.9,\"#4d9221\"],[1,\"#276419\"]],\"sequential\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"sequentialminus\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]},\"colorway\":[\"#636efa\",\"#EF553B\",\"#00cc96\",\"#ab63fa\",\"#FFA15A\",\"#19d3f3\",\"#FF6692\",\"#B6E880\",\"#FF97FF\",\"#FECB52\"],\"font\":{\"color\":\"#2a3f5f\"},\"geo\":{\"bgcolor\":\"white\",\"lakecolor\":\"white\",\"landcolor\":\"#E5ECF6\",\"showlakes\":true,\"showland\":true,\"subunitcolor\":\"white\"},\"hoverlabel\":{\"align\":\"left\"},\"hovermode\":\"closest\",\"mapbox\":{\"style\":\"light\"},\"paper_bgcolor\":\"white\",\"plot_bgcolor\":\"#E5ECF6\",\"polar\":{\"angularaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"radialaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"scene\":{\"xaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"yaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"zaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"}},\"shapedefaults\":{\"line\":{\"color\":\"#2a3f5f\"}},\"ternary\":{\"aaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"baxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"caxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"title\":{\"x\":0.05},\"xaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2},\"yaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2}}},\"xaxis\":{\"anchor\":\"y\",\"domain\":[0.0,1.0],\"title\":{\"text\":\"Anio\"}},\"yaxis\":{\"anchor\":\"x\",\"domain\":[0.0,1.0],\"title\":{\"text\":\"Produccion_Nuclear\"}},\"legend\":{\"title\":{\"text\":\"Pais\"},\"tracegroupgap\":0},\"margin\":{\"t\":60},\"title\":{\"text\":\"Producci\\u00f3n de Energ\\u00eda Nuclear (1995-2021)\"}},                        {\"responsive\": true}                    ).then(function(){\n",
              "                            \n",
              "var gd = document.getElementById('74141ec6-ef77-449c-bb4f-793923126ac8');\n",
              "var x = new MutationObserver(function (mutations, observer) {{\n",
              "        var display = window.getComputedStyle(gd).display;\n",
              "        if (!display || display === 'none') {{\n",
              "            console.log([gd, 'removed!']);\n",
              "            Plotly.purge(gd);\n",
              "            observer.disconnect();\n",
              "        }}\n",
              "}});\n",
              "\n",
              "// Listen for the removal of the full notebook cells\n",
              "var notebookContainer = gd.closest('#notebook-container');\n",
              "if (notebookContainer) {{\n",
              "    x.observe(notebookContainer, {childList: true});\n",
              "}}\n",
              "\n",
              "// Listen for the clearing of the current output cell\n",
              "var outputEl = gd.closest('.output');\n",
              "if (outputEl) {{\n",
              "    x.observe(outputEl, {childList: true});\n",
              "}}\n",
              "\n",
              "                        })                };                            </script>        </div>\n",
              "</body>\n",
              "</html>"
            ]
          },
          "metadata": {}
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "fig = px.choropleth(Energias_Renovables_Fer, locations=\"Codigo_pais\", color='Produccion_Nuclear')\n",
        "fig.update_layout(title_text = 'Producción de Energía Nuclear por país (ExaJoules)')\n",
        "fig.show()"
      ],
      "metadata": {
        "id": "axHIfELYsU8x",
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 542
        },
        "outputId": "1cb82896-076a-494f-a36f-1d07b480237a"
      },
      "execution_count": 20,
      "outputs": [
        {
          "output_type": "display_data",
          "data": {
            "text/html": [
              "<html>\n",
              "<head><meta charset=\"utf-8\" /></head>\n",
              "<body>\n",
              "    <div>            <script src=\"https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.5/MathJax.js?config=TeX-AMS-MML_SVG\"></script><script type=\"text/javascript\">if (window.MathJax) {MathJax.Hub.Config({SVG: {font: \"STIX-Web\"}});}</script>                <script type=\"text/javascript\">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>\n",
              "        <script src=\"https://cdn.plot.ly/plotly-2.8.3.min.js\"></script>                <div id=\"73c053a8-36a0-405c-8a9d-7a66026b5747\" class=\"plotly-graph-div\" style=\"height:525px; width:100%;\"></div>            <script type=\"text/javascript\">                                    window.PLOTLYENV=window.PLOTLYENV || {};                                    if (document.getElementById(\"73c053a8-36a0-405c-8a9d-7a66026b5747\")) {                    Plotly.newPlot(                        \"73c053a8-36a0-405c-8a9d-7a66026b5747\",                        [{\"coloraxis\":\"coloraxis\",\"geo\":\"geo\",\"hovertemplate\":\"Codigo_pais=%{location}<br>Produccion_Nuclear=%{z}<extra></extra>\",\"locations\":[\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"CAN\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"MEX\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"USA\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"ARG\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BRA\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BEL\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"BGR\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"CZE\",\"FIN\",\"FIN\",\"FIN\",\"FIN\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"FRA\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"DEU\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"HUN\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"NLD\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"ESP\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"SWE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"CHE\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"UKR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"GBR\",\"RUS\",\"RUS\",\"RUS\",\"RUS\",\"RUS\",\"RUS\",\"RUS\",\"RUS\",\"RUS\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"0\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"ZAF\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"CHN\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"IND\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"JPN\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"PAK\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"KOR\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"TWN\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"ROU\",\"IRN\",\"IRN\",\"IRN\",\"IRN\",\"IRN\",\"BLR\",\"BLR\"],\"name\":\"\",\"z\":[92.1,82.0,71.0,73.0,72.3,76.2,75.0,74.4,89.8,91.4,97.3,92.8,95.4,89.5,90.0,92.9,94.2,102.7,106.5,101.1,100.7,100.6,100.0,100.5,97.5,92.0,10.5,9.3,10.0,8.2,8.7,9.7,10.5,9.2,10.8,10.9,10.4,9.8,10.5,5.9,10.1,8.8,11.8,9.7,11.6,10.6,10.9,13.6,11.2,11.2,11.9,708.8,710.2,661.7,709.2,766.6,793.6,809.3,821.1,803.9,830.0,823.1,828.7,848.9,848.6,840.9,849.4,831.8,809.8,830.5,839.1,839.1,848.1,847.3,849.6,852.0,831.5,819.1,6.4,6.4,6.2,5.5,7.0,8.3,6.1,6.9,8.4,10.7,10.8,13.4,11.6,9.9,13.8,12.3,14.0,13.0,14.5,15.7,16.0,15.4,15.4,14.7,15.9,15.7,15.7,16.1,14.1,14.7,47.4,47.4,47.3,47.6,46.6,48.2,45.6,47.2,47.9,48.2,40.3,42.6,33.7,26.1,43.5,42.2,28.6,43.5,34.4,50.6,15.3,15.2,16.3,15.8,14.2,15.9,15.4,15.8,15.5,16.1,16.6,16.6,16.5,26.2,26.6,27.2,28.0,28.3,30.3,30.7,30.3,26.8,24.1,28.3,29.9,30.2,30.0,30.7,23.0,24.1,23.5,23.8,395.5,388.0,394.2,415.2,421.1,436.8,441.1,448.2,451.5,450.2,439.7,439.4,409.7,428.5,442.4,425.4,423.7,436.5,437.4,403.2,398.4,412.9,399.0,353.8,379.4,161.6,170.3,161.6,170.0,169.6,171.3,164.8,165.1,167.1,163.0,167.4,140.5,148.8,134.9,140.6,108.0,99.5,97.3,97.1,91.8,84.6,76.3,76.0,75.1,64.4,69.0,14.8,15.4,15.8,15.7,15.8,15.4,15.6,15.8,16.1,16.1,15.7,16.3,16.1,16.0,4.2,2.4,3.8,3.8,3.9,4.0,3.9,4.0,3.8,4.0,3.5,4.2,4.2,4.2,4.0,4.1,3.9,2.9,4.1,4.1,4.0,3.4,3.5,3.9,4.1,3.8,56.3,55.3,59.0,58.9,62.2,63.7,63.0,61.9,63.6,57.5,60.1,55.1,59.0,52.8,61.6,57.7,61.5,56.7,57.3,57.3,58.6,58.1,55.8,58.3,58.3,56.5,74.3,69.9,73.6,73.2,57.3,72.1,68.1,67.4,77.7,72.7,67.0,67.0,63.9,52.2,57.7,60.5,64.0,66.5,64.9,56.3,63.1,65.7,68.5,66.1,49.2,53.1,26.9,25.6,26.2,27.8,23.3,21.3,20.5,24.4,25.3,23.0,18.5,89.2,90.2,90.1,83.2,88.4,87.6,81.0,85.6,84.4,83.0,76.2,86.2,95.1,85.1,90.1,87.8,88.7,80.0,81.6,75.5,63.0,52.5,69.1,62.1,69.0,70.4,70.6,63.7,70.3,71.7,70.3,65.1,56.2,50.3,45.9,172.5,180.8,195.5,196.6,203.1,204.6,209.0,215.9,222.4,2.6,2.5,2.5,2.3,2.4,2.5,2.8,2.4,2.6,2.1,2.2,2.8,2.0,13.5,12.9,13.0,14.1,13.8,12.2,15.0,14.2,11.6,13.6,13.9,10.4,14.3,14.4,14.1,14.9,16.7,17.5,25.1,43.3,50.5,53.1,54.8,62.1,68.4,70.1,74.7,87.2,98.3,111.5,133.2,171.4,213.2,248.1,295.0,348.7,366.2,407.5,15.8,18.9,19.4,18.1,21.3,17.7,17.6,17.8,15.2,16.8,23.1,32.2,33.1,33.3,34.7,38.3,37.9,37.4,39.1,45.2,44.6,43.9,321.2,326.0,317.2,319.1,320.5,314.3,230.1,285.9,293.0,304.3,279.0,251.7,274.7,292.4,162.9,18.0,14.6,4.5,17.7,29.1,49.1,65.6,43.0,61.2,1.7,2.5,2.5,3.9,2.7,4.3,4.7,4.3,5.3,8.1,9.2,9.2,9.5,15.9,109.0,112.1,119.1,129.7,130.7,146.8,148.7,142.9,151.0,147.8,148.6,154.7,150.3,138.8,156.4,164.8,162.0,148.4,133.5,145.9,160.2,158.0,39.5,40.0,39.9,40.5,40.8,41.6,41.6,42.1,40.4,41.6,42.4,36.5,31.7,22.4,27.7,32.3,31.4,27.8,11.7,11.5,11.6,11.7,11.6,11.3,11.5,11.4,11.3,11.5,11.3,7.0,6.9,6.4,6.3,3.5,0.3,5.8],\"type\":\"choropleth\"}],                        {\"template\":{\"data\":{\"bar\":[{\"error_x\":{\"color\":\"#2a3f5f\"},\"error_y\":{\"color\":\"#2a3f5f\"},\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"bar\"}],\"barpolar\":[{\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"barpolar\"}],\"carpet\":[{\"aaxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"baxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"type\":\"carpet\"}],\"choropleth\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"choropleth\"}],\"contour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"contour\"}],\"contourcarpet\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"contourcarpet\"}],\"heatmap\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmap\"}],\"heatmapgl\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmapgl\"}],\"histogram\":[{\"marker\":{\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"histogram\"}],\"histogram2d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2d\"}],\"histogram2dcontour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2dcontour\"}],\"mesh3d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"mesh3d\"}],\"parcoords\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"parcoords\"}],\"pie\":[{\"automargin\":true,\"type\":\"pie\"}],\"scatter\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter\"}],\"scatter3d\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter3d\"}],\"scattercarpet\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattercarpet\"}],\"scattergeo\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergeo\"}],\"scattergl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergl\"}],\"scattermapbox\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattermapbox\"}],\"scatterpolar\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolar\"}],\"scatterpolargl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolargl\"}],\"scatterternary\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterternary\"}],\"surface\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"surface\"}],\"table\":[{\"cells\":{\"fill\":{\"color\":\"#EBF0F8\"},\"line\":{\"color\":\"white\"}},\"header\":{\"fill\":{\"color\":\"#C8D4E3\"},\"line\":{\"color\":\"white\"}},\"type\":\"table\"}]},\"layout\":{\"annotationdefaults\":{\"arrowcolor\":\"#2a3f5f\",\"arrowhead\":0,\"arrowwidth\":1},\"autotypenumbers\":\"strict\",\"coloraxis\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"colorscale\":{\"diverging\":[[0,\"#8e0152\"],[0.1,\"#c51b7d\"],[0.2,\"#de77ae\"],[0.3,\"#f1b6da\"],[0.4,\"#fde0ef\"],[0.5,\"#f7f7f7\"],[0.6,\"#e6f5d0\"],[0.7,\"#b8e186\"],[0.8,\"#7fbc41\"],[0.9,\"#4d9221\"],[1,\"#276419\"]],\"sequential\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"sequentialminus\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]},\"colorway\":[\"#636efa\",\"#EF553B\",\"#00cc96\",\"#ab63fa\",\"#FFA15A\",\"#19d3f3\",\"#FF6692\",\"#B6E880\",\"#FF97FF\",\"#FECB52\"],\"font\":{\"color\":\"#2a3f5f\"},\"geo\":{\"bgcolor\":\"white\",\"lakecolor\":\"white\",\"landcolor\":\"#E5ECF6\",\"showlakes\":true,\"showland\":true,\"subunitcolor\":\"white\"},\"hoverlabel\":{\"align\":\"left\"},\"hovermode\":\"closest\",\"mapbox\":{\"style\":\"light\"},\"paper_bgcolor\":\"white\",\"plot_bgcolor\":\"#E5ECF6\",\"polar\":{\"angularaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"radialaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"scene\":{\"xaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"yaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"zaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"}},\"shapedefaults\":{\"line\":{\"color\":\"#2a3f5f\"}},\"ternary\":{\"aaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"baxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"caxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"title\":{\"x\":0.05},\"xaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2},\"yaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2}}},\"geo\":{\"domain\":{\"x\":[0.0,1.0],\"y\":[0.0,1.0]},\"center\":{}},\"coloraxis\":{\"colorbar\":{\"title\":{\"text\":\"Produccion_Nuclear\"}},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]},\"legend\":{\"tracegroupgap\":0},\"margin\":{\"t\":60},\"title\":{\"text\":\"Producci\\u00f3n de Energ\\u00eda Nuclear por pa\\u00eds (ExaJoules)\"}},                        {\"responsive\": true}                    ).then(function(){\n",
              "                            \n",
              "var gd = document.getElementById('73c053a8-36a0-405c-8a9d-7a66026b5747');\n",
              "var x = new MutationObserver(function (mutations, observer) {{\n",
              "        var display = window.getComputedStyle(gd).display;\n",
              "        if (!display || display === 'none') {{\n",
              "            console.log([gd, 'removed!']);\n",
              "            Plotly.purge(gd);\n",
              "            observer.disconnect();\n",
              "        }}\n",
              "}});\n",
              "\n",
              "// Listen for the removal of the full notebook cells\n",
              "var notebookContainer = gd.closest('#notebook-container');\n",
              "if (notebookContainer) {{\n",
              "    x.observe(notebookContainer, {childList: true});\n",
              "}}\n",
              "\n",
              "// Listen for the clearing of the current output cell\n",
              "var outputEl = gd.closest('.output');\n",
              "if (outputEl) {{\n",
              "    x.observe(outputEl, {childList: true});\n",
              "}}\n",
              "\n",
              "                        })                };                            </script>        </div>\n",
              "</body>\n",
              "</html>"
            ]
          },
          "metadata": {}
        }
      ]
    },
    {
      "cell_type": "markdown",
      "source": [
        "Tablas de energía renovable"
      ],
      "metadata": {
        "id": "oKCOdoorgvTZ"
      }
    },
    {
      "cell_type": "markdown",
      "source": [
        "Contruccion de las columnas Consumo y Produccion Total"
      ],
      "metadata": {
        "id": "7B2-ag4Loz8B"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "#Energias_Renovables.to_csv('Energias_Renovables_Fer.csv',index=False)\n",
        "#Energias_Renovables_Fer=pd.read_csv('Energias_Renovables_Fer.csv')\n",
        "#Energias_Renovables_Fer.to_sql(con=conexion,if_exists='replace',index=False,name='Energias_Renovables_Fer')"
      ],
      "metadata": {
        "id": "Ro2b5C98Up8V"
      },
      "execution_count": 21,
      "outputs": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "Omiti Energia_Geotermica porque no tiene Consumo"
      ],
      "metadata": {
        "id": "k3-Bh4DQB4We"
      }
    },
    {
      "cell_type": "code",
      "source": [
        "df=Energias_Renovables_Fer[Energias_Renovables_Fer['Total_Consumo']>=1.5]\n",
        "df=df.sort_values(by=\"Total_Consumo\", ascending=False)\n",
        "fig=px.bar(df, x='Anio', y='Total_Consumo',color='Pais')\n",
        "fig.update_layout(title_text = 'Consumo Total de Energías Renovables (ExaJoules)')\n",
        "fig.show()"
      ],
      "metadata": {
        "id": "FtmWEl66J9uA",
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 542
        },
        "outputId": "6e4ad11b-7049-4487-b1f4-c2d1becf3b71"
      },
      "execution_count": 22,
      "outputs": [
        {
          "output_type": "display_data",
          "data": {
            "text/html": [
              "<html>\n",
              "<head><meta charset=\"utf-8\" /></head>\n",
              "<body>\n",
              "    <div>            <script src=\"https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.5/MathJax.js?config=TeX-AMS-MML_SVG\"></script><script type=\"text/javascript\">if (window.MathJax) {MathJax.Hub.Config({SVG: {font: \"STIX-Web\"}});}</script>                <script type=\"text/javascript\">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>\n",
              "        <script src=\"https://cdn.plot.ly/plotly-2.8.3.min.js\"></script>                <div id=\"874ffd0d-4240-4a92-affd-c331f4036adc\" class=\"plotly-graph-div\" style=\"height:525px; width:100%;\"></div>            <script type=\"text/javascript\">                                    window.PLOTLYENV=window.PLOTLYENV || {};                                    if (document.getElementById(\"874ffd0d-4240-4a92-affd-c331f4036adc\")) {                    Plotly.newPlot(                        \"874ffd0d-4240-4a92-affd-c331f4036adc\",                        [{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=China<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"China\",\"marker\":{\"color\":\"#636efa\",\"pattern\":{\"shape\":\"\"}},\"name\":\"China\",\"offsetgroup\":\"China\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2019,2018,2017,2016,2015,2014,2013,2012,2011],\"xaxis\":\"x\",\"y\":[12.94,10.2,9.16,7.859999999999999,6.33,4.93,3.770000000000001,3.04,2.49,1.99,1.6],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=United States<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"United States\",\"marker\":{\"color\":\"#EF553B\",\"pattern\":{\"shape\":\"\"}},\"name\":\"United States\",\"offsetgroup\":\"United States\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2019,2018,2017,2016,2015,2014,2013,2011,2012,2010,2009,2008,2007,2006,2004,2002,2005,2001,2003,2000,1999,1996,1995,1998,1997],\"xaxis\":\"x\",\"y\":[12.57,12.02,11.62,11.28,11.0,10.579999999999998,10.06,9.919999999999998,9.64,9.19,9.16,9.13,8.879999999999999,8.83,8.649999999999999,8.44,8.42,8.399999999999999,8.34,8.290000000000001,8.18,8.18,7.89,7.3,7.28,7.28,6.81],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=France<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"France\",\"marker\":{\"color\":\"#00cc96\",\"pattern\":{\"shape\":\"\"}},\"name\":\"France\",\"offsetgroup\":\"France\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2005,2004,2006,2003,2002,2011,2015,2007,2008,2014,2001,2000,2012,2010,2013,2018,2019,1997,1999,2009,2016,2017,1998,2021,2020],\"xaxis\":\"x\",\"y\":[4.48,4.47,4.449999999999999,4.42,4.4,4.359999999999999,4.3500000000000005,4.34,4.329999999999999,4.31,4.27,4.24,4.220000000000001,4.22,4.2,4.140000000000001,4.069999999999999,4.04,4.03,4.03,4.0200000000000005,3.99,3.96,3.92,3.71],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=Japan<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"Japan\",\"marker\":{\"color\":\"#ab63fa\",\"pattern\":{\"shape\":\"\"}},\"name\":\"Japan\",\"offsetgroup\":\"Japan\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[1998,1997,2001,2000,1999,2002,2006,2005,2010,2004,2007,2009,2008,2003,2011],\"xaxis\":\"x\",\"y\":[3.33,3.28,3.26,3.26,3.24,3.18,3.03,2.94,2.88,2.87,2.78,2.7099999999999995,2.5,2.33,1.64],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=Germany<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"Germany\",\"marker\":{\"color\":\"#FFA15A\",\"pattern\":{\"shape\":\"\"}},\"name\":\"Germany\",\"offsetgroup\":\"Germany\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2019,2020,2021,2018,2017,2015,2006,2004,2005,2008,2016,2010,2001,2003,2000,2002,2014,2007,1999,1997,2009,2013,2011,2012,1998,1996],\"xaxis\":\"x\",\"y\":[2.29,2.29,2.1900000000000004,2.15,2.0700000000000003,1.99,1.99,1.94,1.91,1.91,1.9,1.8600000000000003,1.85,1.85,1.83,1.83,1.82,1.81,1.8,1.77,1.77,1.74,1.73,1.71,1.7,1.67],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=Russia<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"Russia\",\"marker\":{\"color\":\"#19d3f3\",\"pattern\":{\"shape\":\"\"}},\"name\":\"Russia\",\"offsetgroup\":\"Russia\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2019,2017,2018,2015,2016,2014,2013],\"xaxis\":\"x\",\"y\":[2.05,1.99,1.91,1.88,1.88,1.82,1.82,1.69,1.62],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=India<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"India\",\"marker\":{\"color\":\"#FF6692\",\"pattern\":{\"shape\":\"\"}},\"name\":\"India\",\"offsetgroup\":\"India\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020],\"xaxis\":\"x\",\"y\":[1.6800000000000002,1.52],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=South Korea<br>Anio=%{x}<br>Total_Consumo=%{y}<extra></extra>\",\"legendgroup\":\"South Korea\",\"marker\":{\"color\":\"#B6E880\",\"pattern\":{\"shape\":\"\"}},\"name\":\"South Korea\",\"offsetgroup\":\"South Korea\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2015,2016,2014],\"xaxis\":\"x\",\"y\":[1.67,1.66,1.58,1.57,1.5],\"yaxis\":\"y\",\"type\":\"bar\"}],                        {\"template\":{\"data\":{\"bar\":[{\"error_x\":{\"color\":\"#2a3f5f\"},\"error_y\":{\"color\":\"#2a3f5f\"},\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"bar\"}],\"barpolar\":[{\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"barpolar\"}],\"carpet\":[{\"aaxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"baxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"type\":\"carpet\"}],\"choropleth\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"choropleth\"}],\"contour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"contour\"}],\"contourcarpet\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"contourcarpet\"}],\"heatmap\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmap\"}],\"heatmapgl\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmapgl\"}],\"histogram\":[{\"marker\":{\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"histogram\"}],\"histogram2d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2d\"}],\"histogram2dcontour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2dcontour\"}],\"mesh3d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"mesh3d\"}],\"parcoords\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"parcoords\"}],\"pie\":[{\"automargin\":true,\"type\":\"pie\"}],\"scatter\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter\"}],\"scatter3d\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter3d\"}],\"scattercarpet\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattercarpet\"}],\"scattergeo\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergeo\"}],\"scattergl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergl\"}],\"scattermapbox\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattermapbox\"}],\"scatterpolar\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolar\"}],\"scatterpolargl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolargl\"}],\"scatterternary\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterternary\"}],\"surface\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"surface\"}],\"table\":[{\"cells\":{\"fill\":{\"color\":\"#EBF0F8\"},\"line\":{\"color\":\"white\"}},\"header\":{\"fill\":{\"color\":\"#C8D4E3\"},\"line\":{\"color\":\"white\"}},\"type\":\"table\"}]},\"layout\":{\"annotationdefaults\":{\"arrowcolor\":\"#2a3f5f\",\"arrowhead\":0,\"arrowwidth\":1},\"autotypenumbers\":\"strict\",\"coloraxis\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"colorscale\":{\"diverging\":[[0,\"#8e0152\"],[0.1,\"#c51b7d\"],[0.2,\"#de77ae\"],[0.3,\"#f1b6da\"],[0.4,\"#fde0ef\"],[0.5,\"#f7f7f7\"],[0.6,\"#e6f5d0\"],[0.7,\"#b8e186\"],[0.8,\"#7fbc41\"],[0.9,\"#4d9221\"],[1,\"#276419\"]],\"sequential\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"sequentialminus\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]},\"colorway\":[\"#636efa\",\"#EF553B\",\"#00cc96\",\"#ab63fa\",\"#FFA15A\",\"#19d3f3\",\"#FF6692\",\"#B6E880\",\"#FF97FF\",\"#FECB52\"],\"font\":{\"color\":\"#2a3f5f\"},\"geo\":{\"bgcolor\":\"white\",\"lakecolor\":\"white\",\"landcolor\":\"#E5ECF6\",\"showlakes\":true,\"showland\":true,\"subunitcolor\":\"white\"},\"hoverlabel\":{\"align\":\"left\"},\"hovermode\":\"closest\",\"mapbox\":{\"style\":\"light\"},\"paper_bgcolor\":\"white\",\"plot_bgcolor\":\"#E5ECF6\",\"polar\":{\"angularaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"radialaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"scene\":{\"xaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"yaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"zaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"}},\"shapedefaults\":{\"line\":{\"color\":\"#2a3f5f\"}},\"ternary\":{\"aaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"baxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"caxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"title\":{\"x\":0.05},\"xaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2},\"yaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2}}},\"xaxis\":{\"anchor\":\"y\",\"domain\":[0.0,1.0],\"title\":{\"text\":\"Anio\"}},\"yaxis\":{\"anchor\":\"x\",\"domain\":[0.0,1.0],\"title\":{\"text\":\"Total_Consumo\"}},\"legend\":{\"title\":{\"text\":\"Pais\"},\"tracegroupgap\":0},\"margin\":{\"t\":60},\"barmode\":\"relative\",\"title\":{\"text\":\"Consumo Total de Energ\\u00edas Renovables (ExaJoules)\"}},                        {\"responsive\": true}                    ).then(function(){\n",
              "                            \n",
              "var gd = document.getElementById('874ffd0d-4240-4a92-affd-c331f4036adc');\n",
              "var x = new MutationObserver(function (mutations, observer) {{\n",
              "        var display = window.getComputedStyle(gd).display;\n",
              "        if (!display || display === 'none') {{\n",
              "            console.log([gd, 'removed!']);\n",
              "            Plotly.purge(gd);\n",
              "            observer.disconnect();\n",
              "        }}\n",
              "}});\n",
              "\n",
              "// Listen for the removal of the full notebook cells\n",
              "var notebookContainer = gd.closest('#notebook-container');\n",
              "if (notebookContainer) {{\n",
              "    x.observe(notebookContainer, {childList: true});\n",
              "}}\n",
              "\n",
              "// Listen for the clearing of the current output cell\n",
              "var outputEl = gd.closest('.output');\n",
              "if (outputEl) {{\n",
              "    x.observe(outputEl, {childList: true});\n",
              "}}\n",
              "\n",
              "                        })                };                            </script>        </div>\n",
              "</body>\n",
              "</html>"
            ]
          },
          "metadata": {}
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "df1=Energias_Renovables_Fer[Energias_Renovables_Fer['Total_Produccion']>=199]\n",
        "df1 = df1.sort_values(by=\"Total_Consumo\", ascending=False)\n",
        "fig1=px.bar(df1, x='Anio', y='Total_Produccion',color='Pais')\n",
        "fig1.update_layout(title_text = 'Producción Total de Energías Renovables (TWh)')\n",
        "fig1.show()"
      ],
      "metadata": {
        "id": "Mf-IniQ7Kv7-",
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 542
        },
        "outputId": "75c8dfc1-b8f8-441d-da92-2e61fc7fa112"
      },
      "execution_count": 23,
      "outputs": [
        {
          "output_type": "display_data",
          "data": {
            "text/html": [
              "<html>\n",
              "<head><meta charset=\"utf-8\" /></head>\n",
              "<body>\n",
              "    <div>            <script src=\"https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.5/MathJax.js?config=TeX-AMS-MML_SVG\"></script><script type=\"text/javascript\">if (window.MathJax) {MathJax.Hub.Config({SVG: {font: \"STIX-Web\"}});}</script>                <script type=\"text/javascript\">window.PlotlyConfig = {MathJaxConfig: 'local'};</script>\n",
              "        <script src=\"https://cdn.plot.ly/plotly-2.8.3.min.js\"></script>                <div id=\"0a8feb49-2043-40a1-9463-138fc4c5fca1\" class=\"plotly-graph-div\" style=\"height:525px; width:100%;\"></div>            <script type=\"text/javascript\">                                    window.PLOTLYENV=window.PLOTLYENV || {};                                    if (document.getElementById(\"0a8feb49-2043-40a1-9463-138fc4c5fca1\")) {                    Plotly.newPlot(                        \"0a8feb49-2043-40a1-9463-138fc4c5fca1\",                        [{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=China<br>Anio=%{x}<br>Total_Produccion=%{y}<extra></extra>\",\"legendgroup\":\"China\",\"marker\":{\"color\":\"#636efa\",\"pattern\":{\"shape\":\"\"}},\"name\":\"China\",\"offsetgroup\":\"China\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2019,2018,2017,2016,2015,2014,2013,2012],\"xaxis\":\"x\",\"y\":[1390.1,1093.8,978.0,837.7,670.5,520.6,396.5,316.5,258.20000000000005,204.9],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=United States<br>Anio=%{x}<br>Total_Produccion=%{y}<extra></extra>\",\"legendgroup\":\"United States\",\"marker\":{\"color\":\"#EF553B\",\"pattern\":{\"shape\":\"\"}},\"name\":\"United States\",\"offsetgroup\":\"United States\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2019,2018,2017,2016,2015,2014,2013,2011,2012,2010,2009,2008,2007,2006,2004,2002,2005,2001,2003,2000,1999,1996,1995,1998,1997],\"xaxis\":\"x\",\"y\":[1368.1,1304.9,1258.9,1219.3,1182.3,1132.8,1071.1,1051.8000000000002,1016.0,957.9,961.0,948.0,917.6,906.1,884.8,856.4,845.0,832.2,841.8000000000001,816.6999999999999,815.8,799.7,771.6,714.0,712.5,712.8000000000001,665.5],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=France<br>Anio=%{x}<br>Total_Produccion=%{y}<extra></extra>\",\"legendgroup\":\"France\",\"marker\":{\"color\":\"#00cc96\",\"pattern\":{\"shape\":\"\"}},\"name\":\"France\",\"offsetgroup\":\"France\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2005,2004,2006,2003,2002,2011,2015,2007,2008,2014,2001,2000,2012,2010,2013,2018,2019,1997,2009,1999,2016,2017,1998,2021,2020],\"xaxis\":\"x\",\"y\":[452.5,448.8,452.4,441.5,437.1,456.6,466.1,443.8,445.1,459.7,421.2000000000001,415.2,444.5,439.0,444.5,451.8,445.3,395.5,417.8,394.2,432.7,432.0,388.0,431.0,406.4],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=Japan<br>Anio=%{x}<br>Total_Produccion=%{y}<extra></extra>\",\"legendgroup\":\"Japan\",\"marker\":{\"color\":\"#ab63fa\",\"pattern\":{\"shape\":\"\"}},\"name\":\"Japan\",\"offsetgroup\":\"Japan\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[1998,1997,2001,2000,1999,2002,2006,2005,2010,2004,2007,2009,2008,2003],\"xaxis\":\"x\",\"y\":[326.1,321.3,321.3,319.50000000000006,317.4,315.4,308.40000000000003,296.5,300.29999999999995,288.6,284.0,281.1,257.2,231.9],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=Germany<br>Anio=%{x}<br>Total_Produccion=%{y}<extra></extra>\",\"legendgroup\":\"Germany\",\"marker\":{\"color\":\"#FFA15A\",\"pattern\":{\"shape\":\"\"}},\"name\":\"Germany\",\"offsetgroup\":\"Germany\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2020,2019,2021,2018,2017,2015,2006,2016],\"xaxis\":\"x\",\"y\":[245.1,245.4,235.7,229.5,219.9,209.6,201.00000000000003,201.2],\"yaxis\":\"y\",\"type\":\"bar\"},{\"alignmentgroup\":\"True\",\"hovertemplate\":\"Pais=Russia<br>Anio=%{x}<br>Total_Produccion=%{y}<extra></extra>\",\"legendgroup\":\"Russia\",\"marker\":{\"color\":\"#19d3f3\",\"pattern\":{\"shape\":\"\"}},\"name\":\"Russia\",\"offsetgroup\":\"Russia\",\"orientation\":\"v\",\"showlegend\":true,\"textposition\":\"auto\",\"x\":[2021,2020,2019,2017,2018],\"xaxis\":\"x\",\"y\":[227.3,218.9,210.3,203.7,205.4],\"yaxis\":\"y\",\"type\":\"bar\"}],                        {\"template\":{\"data\":{\"bar\":[{\"error_x\":{\"color\":\"#2a3f5f\"},\"error_y\":{\"color\":\"#2a3f5f\"},\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"bar\"}],\"barpolar\":[{\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"barpolar\"}],\"carpet\":[{\"aaxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"baxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"type\":\"carpet\"}],\"choropleth\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"choropleth\"}],\"contour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"contour\"}],\"contourcarpet\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"contourcarpet\"}],\"heatmap\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmap\"}],\"heatmapgl\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"heatmapgl\"}],\"histogram\":[{\"marker\":{\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"histogram\"}],\"histogram2d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2d\"}],\"histogram2dcontour\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"histogram2dcontour\"}],\"mesh3d\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"type\":\"mesh3d\"}],\"parcoords\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"parcoords\"}],\"pie\":[{\"automargin\":true,\"type\":\"pie\"}],\"scatter\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter\"}],\"scatter3d\":[{\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatter3d\"}],\"scattercarpet\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattercarpet\"}],\"scattergeo\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergeo\"}],\"scattergl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattergl\"}],\"scattermapbox\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scattermapbox\"}],\"scatterpolar\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolar\"}],\"scatterpolargl\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterpolargl\"}],\"scatterternary\":[{\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"type\":\"scatterternary\"}],\"surface\":[{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"type\":\"surface\"}],\"table\":[{\"cells\":{\"fill\":{\"color\":\"#EBF0F8\"},\"line\":{\"color\":\"white\"}},\"header\":{\"fill\":{\"color\":\"#C8D4E3\"},\"line\":{\"color\":\"white\"}},\"type\":\"table\"}]},\"layout\":{\"annotationdefaults\":{\"arrowcolor\":\"#2a3f5f\",\"arrowhead\":0,\"arrowwidth\":1},\"autotypenumbers\":\"strict\",\"coloraxis\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"colorscale\":{\"diverging\":[[0,\"#8e0152\"],[0.1,\"#c51b7d\"],[0.2,\"#de77ae\"],[0.3,\"#f1b6da\"],[0.4,\"#fde0ef\"],[0.5,\"#f7f7f7\"],[0.6,\"#e6f5d0\"],[0.7,\"#b8e186\"],[0.8,\"#7fbc41\"],[0.9,\"#4d9221\"],[1,\"#276419\"]],\"sequential\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"sequentialminus\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]},\"colorway\":[\"#636efa\",\"#EF553B\",\"#00cc96\",\"#ab63fa\",\"#FFA15A\",\"#19d3f3\",\"#FF6692\",\"#B6E880\",\"#FF97FF\",\"#FECB52\"],\"font\":{\"color\":\"#2a3f5f\"},\"geo\":{\"bgcolor\":\"white\",\"lakecolor\":\"white\",\"landcolor\":\"#E5ECF6\",\"showlakes\":true,\"showland\":true,\"subunitcolor\":\"white\"},\"hoverlabel\":{\"align\":\"left\"},\"hovermode\":\"closest\",\"mapbox\":{\"style\":\"light\"},\"paper_bgcolor\":\"white\",\"plot_bgcolor\":\"#E5ECF6\",\"polar\":{\"angularaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"radialaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"scene\":{\"xaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"yaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"},\"zaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"gridwidth\":2,\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\"}},\"shapedefaults\":{\"line\":{\"color\":\"#2a3f5f\"}},\"ternary\":{\"aaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"baxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"bgcolor\":\"#E5ECF6\",\"caxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"title\":{\"x\":0.05},\"xaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2},\"yaxis\":{\"automargin\":true,\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"zerolinewidth\":2}}},\"xaxis\":{\"anchor\":\"y\",\"domain\":[0.0,1.0],\"title\":{\"text\":\"Anio\"}},\"yaxis\":{\"anchor\":\"x\",\"domain\":[0.0,1.0],\"title\":{\"text\":\"Total_Produccion\"}},\"legend\":{\"title\":{\"text\":\"Pais\"},\"tracegroupgap\":0},\"margin\":{\"t\":60},\"barmode\":\"relative\",\"title\":{\"text\":\"Producci\\u00f3n Total de Energ\\u00edas Renovables (TWh)\"}},                        {\"responsive\": true}                    ).then(function(){\n",
              "                            \n",
              "var gd = document.getElementById('0a8feb49-2043-40a1-9463-138fc4c5fca1');\n",
              "var x = new MutationObserver(function (mutations, observer) {{\n",
              "        var display = window.getComputedStyle(gd).display;\n",
              "        if (!display || display === 'none') {{\n",
              "            console.log([gd, 'removed!']);\n",
              "            Plotly.purge(gd);\n",
              "            observer.disconnect();\n",
              "        }}\n",
              "}});\n",
              "\n",
              "// Listen for the removal of the full notebook cells\n",
              "var notebookContainer = gd.closest('#notebook-container');\n",
              "if (notebookContainer) {{\n",
              "    x.observe(notebookContainer, {childList: true});\n",
              "}}\n",
              "\n",
              "// Listen for the clearing of the current output cell\n",
              "var outputEl = gd.closest('.output');\n",
              "if (outputEl) {{\n",
              "    x.observe(outputEl, {childList: true});\n",
              "}}\n",
              "\n",
              "                        })                };                            </script>        </div>\n",
              "</body>\n",
              "</html>"
            ]
          },
          "metadata": {}
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "\n",
        "#df1=Energias_Renovables_Fer[Energias_Renovables_Fer['Total_Produccion']>=199]\n",
        "\n",
        "min_value = df['Total_Produccion'].min()\n",
        "max_value = df['Total_Produccion'].max()\n",
        "fig = px.choropleth(df, locations=\"Codigo_pais\",\n",
        "                    color=\"Total_Produccion\",\n",
        "                    animation_frame='Anio',\n",
        "                    color_continuous_midpoint = 3,\n",
        "color_continuous_scale=px.colors.sequential.thermal_r,\n",
        "range_color=(min_value,max_value))\n",
        "fig.update_layout(width=800, height=500, title_text = 'Produccion Total de Energías Renovables',font_size=14)\n",
        "st.plotly_chart(fig)"
      ],
      "metadata": {
        "id": "NkML8kkkGX8V",
        "colab": {
          "base_uri": "https://localhost:8080/",
          "height": 204
        },
        "outputId": "dd5b155d-bbf6-424b-8c8e-8847961d5233"
      },
      "execution_count": 24,
      "outputs": [
        {
          "output_type": "error",
          "ename": "NameError",
          "evalue": "ignored",
          "traceback": [
            "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
            "\u001b[0;31mNameError\u001b[0m                                 Traceback (most recent call last)",
            "\u001b[0;32m<ipython-input-24-7357e8ef1354>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m\u001b[0m\n\u001b[1;32m     10\u001b[0m range_color=(min_value,max_value))\n\u001b[1;32m     11\u001b[0m \u001b[0mfig\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mupdate_layout\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mwidth\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m800\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mheight\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m500\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mtitle_text\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0;34m'Produccion Total de Energías Renovables'\u001b[0m\u001b[0;34m,\u001b[0m\u001b[0mfont_size\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0;36m14\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m---> 12\u001b[0;31m \u001b[0mst\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mplotly_chart\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mfig\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
            "\u001b[0;31mNameError\u001b[0m: name 'st' is not defined"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "Energias_Renovables_Fer.sort_values(by='Total_Produccion', ascending=True)"
      ],
      "metadata": {
        "id": "bTKP3ko57Pww"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "Energias_Renovables_Fer[Energias_Renovables_Fer['Pais']=='Other CIS (BP)']"
      ],
      "metadata": {
        "id": "uZx2rys48dMK"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "borrar=Energias_Renovables_Fer[Energias_Renovables_Fer['Pais']=='Other CIS (BP)'].index\n",
        "Energias_Renovables_Fer=Energias_Renovables_Fer.drop(borrar)"
      ],
      "metadata": {
        "id": "VGcY-3vJEJrO"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "er_fer=Energias_Renovables_Fer"
      ],
      "metadata": {
        "id": "fBaAcDuvGxsV"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "er_fer.to_sql(con=conexion,if_exists='replace',index=False,name='er_fer')"
      ],
      "metadata": {
        "id": "3NRFBtr4H0NS"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "kpi.Pais.unique()"
      ],
      "metadata": {
        "id": "i9zo7pdQxAoo"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "#kpi=Energias_Renovables_Fer.groupby(['Pais','Anio'],as_index=False).sum().sort_values('Total_Consumo', ascending=False)\n",
        "#kpi = Energias_Renovables_Fer[['Anio','Pais','Total_Consumo']].copy()\n",
        "#kpi[kpi.Pais=='China']\n"
      ],
      "metadata": {
        "id": "B7kwPptkwNqk"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "(kpi[kpi.Pais=='China'].sort_values(by='Anio')['Total_Consumo'][i]-kpi[kpi.Pais=='China'].sort_values(by='Anio')['Total_Consumo'][i+1])/kpi[kpi.Pais=='China'].sort_values(by='Anio')['Total_Consumo'][i+1]"
      ],
      "metadata": {
        "id": "QUO6FE71897i"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "China=Energias_Renovables_Fer[Energias_Renovables_Fer['Pais']=='China']"
      ],
      "metadata": {
        "id": "b_AkHunU9G23"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "China.reset_index(inplace=True,drop=True)"
      ],
      "metadata": {
        "id": "pH-sfyxK9NyD"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "China"
      ],
      "metadata": {
        "id": "50xyHVCc9XdG"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "China"
      ],
      "metadata": {
        "id": "CDS8Zepn-byZ"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "lista=[]\n",
        "for i in range(0,25):\n",
        "  lista.append(round((China['Total_Produccion'][i+1]-China['Total_Produccion'][i])*100/China['Total_Produccion'][i],2))\n",
        "lista"
      ],
      "metadata": {
        "id": "jL0PNtUdwYQ0"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "años=[]\n",
        "for i in range(1997,2022):\n",
        "  años.append(i)\n",
        "años"
      ],
      "metadata": {
        "id": "RqQKb4P8DmOG"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [
        "df=pd.DataFrame(columns=lista,)"
      ],
      "metadata": {
        "id": "E7-pdtnBDyLM"
      },
      "execution_count": null,
      "outputs": []
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "FbBecVSGD5fy"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}
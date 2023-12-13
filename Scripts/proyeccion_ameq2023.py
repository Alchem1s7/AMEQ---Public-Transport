# Importación de las librerías
import pandas as pd
import numpy as np
import os
import time

# Creación de una función que será llamada desde el archivo principal
def proyecciones_ameq2023(input_file_xlsx = str):
    """
    Esta función recibe únicamente el path que señala al archivo de proyecciones del AMEQ
    Su función es aglomerar todas las proyecciones por ruta (ya que este archivo es un xlsx
    de 68 páginas), una vez concentrada la información se hacen algunas transformaciones, y se
    regresa el dataframe."""


    df = pd.read_excel(r"{}".format(input_file_xlsx), sheet_name=None) #Se crea un diccionario con los dfs por ruta

    bad_keys = ["Pob","CR","GDSA","BD"] # Se identifican las llaves que no necesitamos
    clean_dict = {key:value for key,value in df.items() if key not in bad_keys}
    print(f"Se tienen {len(clean_dict.keys()) } rutas distintas" )

        
    df_list = [] # Creamos una lista de dataframes para concatenarlos:

    # Se realizan transformaciones y limpiezas para cada dataframe
    for key in clean_dict.keys():

        data = clean_dict[key]
        data = data.iloc[:,:9]
        data["Ruta"].fillna(method="ffill",inplace=True)
        # Eliminamos registros malos:
        data = data[~(data.Ejercicio == "Ejercicio")]
        data = data[~(data.PERIODO.isin(["Desviacion","Intervalo"]))]

        df_list.append(data)
    print("Tenemos {} dataframes".format(len(df_list)))

    # Se concatenan nuestros dataframes en uno solo:
    df = pd.concat(df_list,ignore_index=True)

    # Creamos una columna de fecha a partir de otras columnas:
    df.Ejercicio = df.Ejercicio.astype(str)
    df.Periodo = df.Periodo.astype(str)
    df["Fecha"] = df["Ejercicio"] + "-" + df["Periodo"] + "-01"
    df.loc[df.Fecha.str.startswith("nan"),"Fecha"] = None # Eliminamos registros no válidos
    df.Fecha = pd.to_datetime(df.Fecha, format= "%Y-%m-%d" )

    # Dropeamos columnas que no son de interés
    df.drop(columns=["PERIODO","Ejercicio","Periodo","Int Inf","Int Sup","n","Conexión","on/off line"], 
            inplace=True)

    # Tratamiento de la columna ruta:
    df.Ruta = df.Ruta.astype(str)
    df.Ruta = df.Ruta.str.replace(".0","", regex = False)

    return df

    # Se debe tener cuidado al consultar los datos ya que la columna de Total pasaje
    # Actúa como encadenante de dos conceptos, total de pasaje real y predicciones
    # Las predicciones son apartir de enero de 2023



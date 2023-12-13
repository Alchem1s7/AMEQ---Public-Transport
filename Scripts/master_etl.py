import pandas as pd
import numpy as np
import os
from master_format import monthly_batch as mb
from final_format_dates import formating, every_row
from ponderada_9 import generacion_base_ponderada as gbp
from proyeccion_ameq2023 import proyecciones_ameq2023
import datetime

def etl_transporte():
        
    ###################################################################################################
    ##################################   RUTAS DE ACCESO  #############################################
    ###################################################################################################
    
    path = input("\n Hola, por favor ingrese la ruta principal (carpeta 'Inputs'): \n")
    outputpath = input("\n Ingrese la carpeta donde desea se guarden los archivos : \n")
    
    
    path2020 = path + "/" + "2020"
    path2021 = path + "/" + "2021"
    path2022 = path + "/" + "2022"
    path2023 = path + "/" + "2023"
    
    
    path_col_names = path + "/Archivos de nombres de columnas"
    csvs_col_names_year = [path_col_names + "/" + file for file in os.listdir(path_col_names) if file.endswith(".csv")]
    ingreso_costo_path = path + "/ingresos_costoss.csv"


    for path_ in csvs_col_names_year:
        
        if "cols_files_2020" in path_:
            path_cols_2020 = path_

        if "cols_files_2021" in path_:
            path_cols_2021 = path_

        if "cols_files_2022" in path_:
            path_cols_2022 = path_            

        if "rename_2020" in path_:
            rename_path20 = path_

        if "rename_2021" in path_:
            rename_path21 = path_  


    cols_2022 = open(file=r"{}".format(path_cols_2022))
    cols_to_keep_2022 = [col[:-1] for col in cols_2022]
    
    cols_2021 = open(file=r"{}".format(path_cols_2021))
    cols_to_keep_2021 = [col[:-1] for col in cols_2021]
    
    cols_2020 = open(file=r"{}".format(path_cols_2020))
    cols_to_keep_2020 = [col[:-1] for col in cols_2020]
    
    rename_2020 = open(file=r"{}".format(rename_path20))
    rename_2020_cols = [col[:-1] for col in rename_2020]
    
    rename_2021 = open(file=r"{}".format(rename_path21))
    rename_2021_cols = [col[:-1] for col in rename_2021]
    
    
    
    ###################################################################################################
    ##################################   ACUMULADO 2020  ##############################################
    ###################################################################################################
    
    # Se leen los archivos mensuales manteniendo solo las columnas de interés
    # En el 2020 no se utilizan los módulos creados
    
    files2020 = os.listdir(path2020)
    dflist = [pd.read_excel(path2020 + "/" + file) for file in files2020 if file.endswith(".xls")]
    dfs2020 = []
    for df in dflist:

        df.columns = df.iloc[0,:]
        df = df.loc[1:,cols_to_keep_2020]
        dfs2020.append(df)
    
    print(
    """*********************************
    LOS DATOS DEL 2020 SE HAN CARGADO
    *********************************""")
    
    
    ###################################################################################################
    ##################################   ACUMULADO 2021  ##############################################
    ###################################################################################################
    
    
    # La funcion mb devuelve una lista de dataframes con los acumulados mensuales
    dfs_list_2021 = mb(path=path2021,year=2021,path_col_names=path_col_names)
    
    print(
    """*********************************
    LOS DATOS DEL 2021 SE HAN CARGADO
    *********************************""")
    
    
    ###################################################################################################
    ##################################   ACUMULADO 2022  ##############################################
    ###################################################################################################
    
    # La funcion mb devuelve una lista de dataframes con los acumulados mensuales
    dfs_list_2022 = mb(path2022,ex=True,path_col_names=path_col_names)
    # Se genera una lista de tuplas conteniendo el shape de cada df
    # Este paso es generado porque esta lista devuelve dataframes duplicados
    # Y el objetivo de este codigo es depurarlos
    shapes= [i.shape for i in dfs_list_2022]
    # Se genera una lista de indexes para las tuplas unicas
    # Set shapes genera un set de valores únicos, de tuplas únicas. 
    indexes=[shapes.index(i) for i in set(shapes)]
    # Se anexa cada dataframe unico en una nueva lista
    dfs_list_2022_unique = [dfs_list_2022[index] for index in indexes]
        
    print(
    """*********************************
    LOS DATOS DEL 2022 SE HAN CARGADO
    *********************************""")
    
    
    ###################################################################################################
    ##################################   ACUMULADO 2023  ##############################################
    ###################################################################################################
    
    
    # La funcion mb devuelve una lista de dataframes con los acumulados mensuales
    dfs_list_2023 = mb(path = path2023,
                        year = 2023,
                        path_col_names = path_col_names)
    

    print(
    """*********************************
    LOS DATOS DEL 2023 SE HAN CARGADO
    *********************************""")
    
    ###################################################################################################
    ##################################   TODOS LOS AÑOS  ##############################################
    ###################################################################################################


    # En la siguiente linea de codigo, se añaden todos los dataframes de todos los años
    every_dataframe_all_years = dfs2020 + dfs_list_2021 + dfs_list_2022_unique + dfs_list_2023
    # En la lista siguiente, se añadirán los dataframes ya normalizados
    every_dataframe_all_years_normalized = []
    
    # En el siguiente ciclo, se verifica el número de columnas de cada dataframe, de esta manera podemos 
    # darnos cuenta de a que año pertenecen y por tanto, que columnas son las que tiene.
    for data in every_dataframe_all_years:
    
        if data.shape[1]==53:
            # para 2020
            data.columns = rename_2020_cols
            every_dataframe_all_years_normalized.append(data)
            
        if data.shape[1]==55:
            # para 2021
            data.columns = rename_2021_cols
            every_dataframe_all_years_normalized.append(data)
            
        if data.shape[1] == 62:
            # Al año 2022 no se le cambia el nombre de sus columnas, 
            # ya que fue el año tomado para la normalización de los nombres.
            every_dataframe_all_years_normalized.append(data)
    
    # Se crea el dataframe final con todos los años
    final = pd.concat(every_dataframe_all_years_normalized,ignore_index=True)
    final.reset_index(drop=True,inplace=True)
    
    
    print(
    """******************************************
    SE HA CREADO EL ACUMULADO DE TODOS LOS AÑOS
    *********************************************""")
    
    ###################################################################################################
    ###############################  NORMALIZACIÓN DE FECHAS  #########################################
    ###################################################################################################
    
    #La función formating quita espacios y 
    final = formating(final)
    # Para todas aquellas fechas que comiencen por el año, se aplica la función every row
    # que intercambia los apartados y devuelve una fecha en el formato dd-mm-yyyy
    
    final.loc[final["Fecha"].str.startswith("2023",na=False),"Fecha"] = final["Fecha"].apply(every_row)
    final.loc[final["Fecha"].str.startswith("2022",na=False),"Fecha"] = final["Fecha"].apply(every_row)
    final.loc[final["Fecha"].str.startswith("2021",na=False),"Fecha"] = final["Fecha"].apply(every_row)
    final.loc[final["Fecha"].str.startswith("2020",na=False),"Fecha"] = final["Fecha"].apply(every_row)
    
    
    
    
    ###################################################################################################
    #################################   REEMPLAZO DE RUTAS  ###########################################
    ###################################################################################################
    
    
    #Importantísimo ya que al momento de reemplazar o hacer strip, 
    # se pueden perder datos por el mix de tipos de datos en la columna Ruta:
    final.Ruta = final.Ruta.astype(str)
    final.loc[final.Ruta=="121 D","Ruta"] = "121D"
    #Hasta aqui, tenemos la base de datos sin reemplazar las rutas por los ID's
    
    #Aqui se reemplazan las rutas
    rutas_to_replace_11 = { "76":"C21","L04":"C22","65":"C23","E01":"T01",
                           "75":"L53","77":"L54","79":"L55","94":"L56","53":"T02",
                           "24":"C24","78":"C25"}
    
    final.Ruta = final.Ruta.replace(rutas_to_replace_11)
    
    # Creamos la columna bloque 2 y bloque 1:
    bloque2 = [ "nan","10","105","121D","123A","123B","130","131","136","14","C24","31","33","38","40","43",
                "44","50","T02","54","55","58","66","67","70","72","C25","80","81","83","85","87","92","98",
                "A2","C23","L51","L52","L53","L55","L56","L57","L58","T01","51C","69","69B","123"]
    
    bloque_1 = ["110","12","121","122","132","134","17","19","21","27","28","29","36","37","41","45",
                "51","56","59","61","62","7","84","9","96","C22","L07","L54","77b"]
    
    # Creamos las columnas de bloque 1 y bloque 2:
    
    final["Bloque 2"] = False
    final["Bloque 1"] = False
        
    final.loc[final.Ruta.isin(bloque2),"Bloque 2"] = True
    final.loc[final.Ruta.isin(bloque_1),"Bloque 1"] = True
    
    print(
    """***********************************************
    SE HAN REEMPLAZADO RUTAS Y MAPEADO CON ID's
    **************************************************""")
    
    
    ###################################################################################################
    #################################   MODELO RELACIONAL  ############################################
    ###################################################################################################
    
    
    
    #*************************************************************
    #********************  PATIO BASE DIM  ***********************
    #*************************************************************
    
    patiobase_unique = final["Patio base"].unique()
    patio_base_dict = {i:j for i,j in zip(patiobase_unique,range(len(patiobase_unique)))}
    patio_base_dim_df = pd.DataFrame(patio_base_dict.items(),columns=["Patio_base","id_Patio_base"])
    
    #Aqui se mapea el patio base
    final["Patio base"] = final["Patio base"].map(patio_base_dict)
    
    #Se carga la tabla ingresos costos:
    ingresos_costosdf = pd.read_csv(ingreso_costo_path, encoding="utf-8")
    ingresos_costosdf = ingresos_costosdf.iloc[:,:5]
    
    #*************************************************************
    #***************  CREACION DE CONCEPTO DIM  ******************
    #*************************************************************
    
    concepto_unique = ingresos_costosdf["Concepto"].unique()
    concepto_dict = {i:j for i,j in zip(concepto_unique,range(len(concepto_unique)))}
    concepto_dim_df = pd.DataFrame(concepto_dict.items(),columns=["Concepto","id_Concepto"])
    
    #Mapeo de la columna id_concepto en la tabla concepto_dim
    
    ingresos_costosdf["Concepto"] = ingresos_costosdf["Concepto"].map(concepto_dict)
    ingresos_costosdf.drop(columns=["Año","Mes"],inplace=True)
    ingresos_costosdf.rename(columns={" Valor ": "Valor"},inplace=True)
    ingresos_costosdf["Valor"] = ingresos_costosdf["Valor"].str.replace("$-","0",regex=True)
    
    #*************************************************************
    #******************  CREACION DE RUTA DIM  *******************
    #*************************************************************
    
    # Creamos las nueva tabla dimensional de rutas:
    rutas_unique = final.Ruta.unique()
    rutas_dim_dict11 = {ruta:id for ruta,id in zip( rutas_unique , range(0,len(rutas_unique)+1) )}
    
    dim_rutas = pd.DataFrame(rutas_dim_dict11.items(), 
                            columns=["Ruta","id_ruta"])
    
    #Ahora se mapean los ids correspondientes a cada ruta:
    final.Ruta = final.Ruta.map(rutas_dim_dict11)
    # Ahora se creará una columna adicional en el dimensional de rutas, llamada reestructura
    # Actualización correspondiente al 8 de diciembre del 2023
    # path es la ruta correspondiente a la carpeta inputs
    reestructura = pd.read_excel(path + "/Reestructura.xlsx")

    # Quitamos los registros con NA en Ruta Original
    df = reestructura[~(reestructura["Ruta Original"].isna())]
    # Hacemos filtrados y reemplazos
    df.loc[:,"Ruta Nueva"] = df["Ruta Nueva"].str.replace("-","", regex=True) # Reemplazamos guiones
    df.loc[:,"Ruta Original"] = df["Ruta Original"].replace("-",np.nan) # Reemplazamos guiones
    df = df[~(df["Ruta Nueva"] == "")] # Quitamos aquellas rutas que desaparecen de nuestro mapeo

    # Ahora tenemos valores NaN en Ruta Original, esto no debe ocurrir así que filtramos una segunda ocasión
    df = df[(df["Ruta Original"].notna()) & (df["Ruta Nueva"].notna())]
    df["Ruta Nueva"] = df["Ruta Nueva"].astype(str)
    df["Ruta Original"] = df["Ruta Original"].astype(str)

    # Hacemos un diccionario dónde en las keys tenemos las rutas originales
    dict_ruta_original = df[["Ruta Nueva", "Ruta Original"]].set_index("Ruta Original")["Ruta Nueva"].to_dict()

    # Hacemos un diccionario dónde en las eys tenemos las rutas nuevas
    dict_ruta_nueva = {value:key for key,value in dict_ruta_original.items()}

    # Ya tenemos listo nuestro diccionario que mapeará las rutas en el dim_rutas
    dim_rutas["Ruta original DL"] = dim_rutas["Ruta"].copy() # Guardamos las rutas originales por cualquier cosa
    # Creamos una nueva columna llamada 'Reestructura', dónde mapearemos las rutas nuevas
    dim_rutas["Reestructura"] = dim_rutas["Ruta"].replace(dict_ruta_original) # Rutas viejas son reemplazadas

    # 'Ruta', contiene tanto rutas nuevas, como rutas viejas, el mapeo que se acaba de realizar
    # solamente cubrirá las rutas viejas, para las rutas nuevas lo que se planea hacer es, mapear a la inversa
    # con el otro diciconario creado, de modo que tengamos rutas originales para ser mapeadas de nuevo con
    # rutas nuevas.
    # Mapeamos las rutas nuevas de la columna Ruta:
    dim_rutas["Ruta"] = dim_rutas["Ruta"].replace(dict_ruta_nueva) # Encontramos rutas nuevas y asignamos las originales
    dim_rutas["Reestructura"] = dim_rutas["Ruta"].replace(dict_ruta_original) #Los valores de rutas nuevas que se mapearon 
    # con rutas originales, ahora son mapeados nuevamente con rutas nuevas

    # Dropeamos Ruta, ya que fue manoseada y contiene valores únicame viejos de rutas
    dim_rutas.drop(columns=["Ruta"], inplace=True)
    # Establecemos valores para las casillas en NaN
    dim_rutas.loc[dim_rutas["Reestructura"].isna(), "Reestructura"] = "Sin ruta detectada"
    dim_rutas.loc[dim_rutas["Ruta original DL"].isna(), "Ruta original DL"] = "Sin ruta detectada"
    # Ahora haremos un merge con la tabla de reestructura, por medio de la llave de reestructura
    dim_rutas_merged = dim_rutas.merge(df[["Concesionario.1","Fecha de intervención","Inicia Operación AMEQ","Ruta Nueva","Modificación"]],
                    left_on="Reestructura", right_on="Ruta Nueva", how="left")\
                        .drop(columns=["Ruta Nueva"])
    
    # Realizamos algunas transformaciones antes de cargar al dashboard:
    dim_rutas_merged["Fecha de intervención"] = dim_rutas_merged["Fecha de intervención"].replace("Apertura 5F", np.nan)
    dim_rutas_merged["Inicia Operación AMEQ"] = dim_rutas_merged["Inicia Operación AMEQ"].astype(str).str.replace("00:00:00", "",regex=True)
    dim_rutas_merged["Inicia Operación AMEQ"] = dim_rutas_merged["Inicia Operación AMEQ"].astype(str)
    dim_rutas_merged["Inicia Operación AMEQ"] = pd.to_datetime(dim_rutas_merged["Inicia Operación AMEQ"], errors="raise")

    diccionario_rutas_divididas = {"12":"12 y C26","C26":"12 y C26", "L61":"L61 y L156", "L156":"L61 y L156",
                               "40":"40 y C30", "C30":"40 y C30", "C39":"C39 y L154","L154":"C39 y L154",
                               "L62":"L62 y L155","L155":"L62 y L155", "C32":"C32 y L108","L108":"C32 y L108",
                               "C65":"C65 y L112","L112":"C65 y L112", "C28":"C28 y L160", "L160":"C28 y L160"
                               }
    # Ahora creamos una columna extra para unificar estas rutas que estan divididas en una sola ruta
    dim_rutas_merged["Reestructura con rutas divididas"] = dim_rutas_merged["Reestructura"].replace(diccionario_rutas_divididas)


    print(
    """*******************************
    CREACIÓN DEL MODELO RELACIONAL
    **********************************""")

    ###################################################################################################
    #########################   ARCHIVOS ADICIONALES PARA EL DASHBOARD  ###############################
    ###################################################################################################

    proyeccionesameq_df2023 = proyecciones_ameq2023(input_file_xlsx = path + "/01. Proyección_AMEQ_Ene2023 (2).xlsx") # Esta función recibe solamente como argumento el path
    # Para la escritura del archivo de las proyecciones.

    ponderada_sin_rutas_df = gbp(path_excel= path + "/Tarifa ponderada sin rutas - Base completa para el dashboard.xlsx")



    print(
    """********************************************************
    La base ponderada y las proyecciones del AMEQ se cargaron
    ***********************************************************""")

    ###################################################################################################
    ######################################   OUTPUTS  #################################################
    ###################################################################################################
    
    actual_time = datetime.datetime.now()
    dia = actual_time.day
    mes = actual_time.month
    año = actual_time.year




    patio_base_dim_df.to_csv(outputpath + "/DIM_patio_base.csv", index=False)
    print("Dimensional patio base ha sido escrita")
    
    concepto_dim_df.to_csv(outputpath + "/DIM_concepto.csv", index=False)
    print("Dimensional concepto ha sido escrita")
    
    dim_rutas_merged.to_csv(outputpath + "/DIM_rutas.csv", index=False)
    print("Dimensional de rutas ha sido escrita")
    
    ingresos_costosdf.to_csv(outputpath + "/FACT_ingresos_costos.csv", index=False)
    print("La tabla de hechos, ingresos costos ha sido escrita")
    
    final.to_csv(outputpath + "/FACT_acumulado_DL.csv", index=False)
    print("La tabla de hechos, diario liquidación con rutas reemplazadas ha sido escrita")

    proyeccionesameq_df2023.to_csv(outputpath + "/proyecciones_AMEQ.csv", index=False)
    print("La tabla de las predicciones para el 2023 del AMEQ ha sido escrita")

    ponderada_sin_rutas_df.to_csv(outputpath + "/FACT_ponderada_sin_rutas.csv", index=False)
    print("La base ponderada ha sido actualizada correctamente en el path de salida especificado")

    
    print(
    """
    
    *******************************
              ALL DONE BABY
    *******************************""")

    
if __name__ == "__main__":
    
    etl_transporte()

    
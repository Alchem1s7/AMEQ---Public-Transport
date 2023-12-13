import pandas as pd
import os


def monthly_batch(path, ex=False, year=2022,path_col_names=str):
    

    """
    Esta función recibe dos argumentos:
    path:   String type, la ruta de acceso a una carpeta que dentro de ella contiene
            subcarpetas que a su vez contienen archivos de excel xlsx o xls, que, en realidad
            son archivos html.

    ex:     Boolean, default: False. Es una variable la cual nos permite tratar adecuadamente al año 2022. 
            Ya que algunos archivos vienen en formato xlsx el cual debe ser tratado diferente.
            Los meses del 2022 son: agosto, septiembre.

    year:   El año de la información. Default 2022, Diciembre de 2021 tiene un tratado especial
            """
    

    # Se define una lista de directorios de subcarpetas mensuales:
    list_of_months = next(os.walk(path))[1]

    # Se define la lista que contendrá los df's mensuales ya normalizados:
    all_months_dfs = []
    
    # Se definen las columnas que nos interesa conservar:
    csvs_col_names_year = [path_col_names +"/"+ file for file in os.listdir(path_col_names) if file.endswith(".csv")]

    for path_ in csvs_col_names_year:
        
        if "cols_files_2020" in path_:
            path_cols_2020 = path_

        if "cols_files_2021" in path_:
            path_cols_2021 = path_

        if "cols_files_2022" in path_:
            path_cols_2022 = path_           


    cols_2022 = open(file=r"{}".format(path_cols_2022))
    cols_to_keep_2022 = [col[:-1] for col in cols_2022]
    
    cols_2021 = open(file=r"{}".format(path_cols_2021))
    cols_to_keep_2021 = [col[:-1] for col in cols_2021]
    
    cols_2020 = open(file=r"{}".format(path_cols_2020))
    cols_to_keep_2020 = [col[:-1] for col in cols_2020]


    if year == 2023: # Caso especial donde se tiene un archivo de diario d liquidación xlsx para todos los días del mes de febrero.

        path_febrero = path + "/" + [item for item in os.listdir(path) if "liquidacion_febrero" in item][0] #Se sabe de antemano que solamente se tiene un archivo xlsx para febrero
        feb2023 = pd.read_excel(path_febrero,sheet_name=None) # Devuelve un diccionario de dataframes para cada sheet de excel
        
        dfs_feb2023 = [] # Se crea una lista para todos los días del mes de febrero

        for key in feb2023.keys():

            data = feb2023[key]
            
            data.columns = data.iloc[0,:]
            data = data.iloc[1:-1,:]
            data = data[cols_to_keep_2022]

            dfs_feb2023.append(data)
        
        febdf = pd.concat(dfs_feb2023)

        all_months_dfs.append(febdf)

        print(f"\nPara el año 2023, se creó el conglomerado de febrero. con el shape: {febdf.shape}\n")

    # Para cada directorio mensual en nuestro listado:
    for name_month in list_of_months:

        # Se define el path mensual concatenando el path principal con el directorio mensual
        monthpath = path + "/" + name_month
        # Los meses definidos a continuación, son aquellos que deben ser tratados distinto en 2022
        meses = ["agosto","septiembre"]

        # Si el nombre actual del directorio mensual es igual a algun mes dentro de la lista
        # meses, significa que este directorio será tratado distinto, aplica para el año 2022
        if ex == True and any(month in name_month.lower() for month in meses):
            
            #Se guardan rutas a archivos y se crean df's diarios
            list_special_months = [file for file in os.listdir(monthpath) if file.endswith(".xls")]

            #list_special_months_dfs #= [pd.read_excel(monthpath + "/" + file, usecols=cols_to_keep) for file in list_special_months]
            
            list_special_months_dfs = []
            for file in list_special_months:
                print(file)
                df = pd.read_excel(monthpath + "/" + file, usecols=cols_to_keep_2022)
                df = df[df["Patio base"]!="Patio base"]
                df.dropna(inplace=True,how="all")
                list_special_months_dfs.append(df)

            
            #Se crea el acumulado mensual y se anexa a la lista que contendrá cada acumulado mensual
            df = pd.concat(list_special_months_dfs)
            all_months_dfs.append(df)
            print("Para la carpeta: ",name_month," se concatenaron {} df's".format(len(list_special_months_dfs) ))
        
        # Si no es 2022 "y" no son los meses especiales:
        else:
            
            # Se crean listas para cada tipo de archivo que se encuentre dentro de un
            # directorio mensual
            
            list_html = [file for file in os.listdir(monthpath) if file.endswith(".xls")]
            list_xlsx = [file for file in os.listdir(monthpath) if file.endswith(".xlsx")]

            

            if len(list_xlsx) != 0: # Si se encontraron archivos "xlsx":
                
                # Se normalizan los dataframes diarios y se concatenan creando el df mensual exceldf
                exceldf_list = []
                for file in list_xlsx:
                    df=pd.read_excel(monthpath + "/" + file)
                    df.columns = df.iloc[0,:]
                    if year==2021:
                        if "diciembre" in name_month.lower():
                            dec2021 = df.loc[1:,cols_to_keep_2022]
                            dec2021 = dec2021.iloc[:-1,:]
                            dec2021 = dec2021[dec2021["Patio base"]!="Patio base"]
                            dec2021 = dec2021[dec2021["Fecha"]!="NaT"]
                            exceldf_list.append(dec2021)
                        else:
                            if "enero" in name_month.lower():
                                ene2021 = df.loc[1:,cols_to_keep_2020]
                                ene2021 = ene2021.iloc[:-1,:]
                                ene2021 = ene2021[ene2021["Patio base"]!="Patio base"]
                                ene2021 = ene2021[ene2021["Fecha"]!="NaT"]
                                exceldf_list.append(ene2021)
                            else:
                                df = df.loc[1:,cols_to_keep_2021]
                                df = df.iloc[:-1,:]
                                df = df[df["Patio base"]!="Patio base"]
                                df = df[df["Fecha"]!="NaT"]
                                exceldf_list.append(df)
                    else:
                        df = df.loc[1:,cols_to_keep_2022]
                        df = df.iloc[:-1,:]
                        df = df[df["Patio base"]!="Patio base"]
                        df = df[df["Fecha"]!="NaT"]
                        exceldf_list.append(df)
            
                exceldf = pd.concat(exceldf_list)
                
            # Si se encuentran archivos html:
            if len(list_html) != 0:
                #htmlsdf_list = [pd.read_html(monthpath + "/" + file)[0] for file in list_html]
                htmlsdf_list=[]
                for file in list_html:
                    print(file)
                    df = pd.read_html(monthpath + "/" + file)[0]
                    df.columns = df.iloc[1,:]
                    if year==2021:
                        if "diciembre" in name_month.lower():
                            df = df.loc[2:,cols_to_keep_2022]
                            df = df.iloc[:-1,:]
                            df = df[df["Patio base"]!="Patio base"]
                            df = df[df["Fecha"]!="NaT"]
                            htmlsdf_list.append(df)
                        # Si el año es 2021, pero no es diciembre:
                        else:
                            if "enero" in name_month.lower():
                                ene2021 = df.loc[2:,cols_to_keep_2020]
                                ene2021 = ene2021.iloc[:-1,:]
                                ene2021 = ene2021[ene2021["Patio base"]!="Patio base"]
                                ene2021 = ene2021[ene2021["Fecha"]!="NaT"]
                                htmlsdf_list.append(ene2021)
                            else:
                                df = df.loc[2:,cols_to_keep_2021]
                                df = df.iloc[:-1,:]
                                df = df[df["Patio base"]!="Patio base"]
                                df = df[df["Fecha"]!="NaT"]
                                htmlsdf_list.append(df)   
                    # Si el año no es 2021
                    else:
                        df = df.loc[2:,cols_to_keep_2022]
                        df = df.iloc[:-1,:]
                        df = df[df["Patio base"]!="Patio base"]
                        df = df[df["Fecha"]!="NaT"]
                        htmlsdf_list.append(df)

                htmldf = pd.concat(htmlsdf_list)
            
            # Si se encontraron archivos con ambas extensiones en un mismo directorio mensual:
            if len(list_xlsx) != 0 and len(list_html) != 0:
                # Se concatenan y se crea un solo df
                df = pd.concat([exceldf, htmldf])
                print("Para la carpeta: ",name_month," se concatenaron {} df's html y {} df's xlsx".format(len(htmlsdf_list),len(exceldf_list) ))
            # Si solo se encontraron archivos xls o "html" en el directorio mensual, se renombra el df:
            if len(list_xlsx) == 0:
                df = htmldf
                print("Para la carpeta: ",name_month," se concatenaron {} df's html".format(len(htmlsdf_list) ))
            # Si se encontraron solamente archivos de excel "xlsx", en el directorio mensual, se renombra el df
            if len(list_html) == 0:
                df = exceldf
                print("Para la carpeta: ",name_month," se concatenaron {} df's xlsx".format(len(exceldf_list) ))
            
            
        # Se anexa el df mensual para cada iteración
        all_months_dfs.append(df)
    
    if year == 2023:
        print(len(all_months_dfs))
    

    return all_months_dfs
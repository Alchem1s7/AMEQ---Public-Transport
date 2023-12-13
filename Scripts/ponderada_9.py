import pandas as pd
import datetime

def generacion_base_ponderada(path_excel=str):

    

    # lectura y transformacion de los datos:
    bd = pd.read_excel(path_excel,
                    sheet_name="Tarifa ponderada")

    #bd = bd[["FECHA","ASCENSOS DEL DÍA","DINERO RECIBIDO directamente"]].copy()
    bd.rename(columns={"ASCENSOS DEL DÍA":"Ascensos",
                    "DINERO RECIBIDO directamente":"Ingreso",
                    "FECHA":"Fecha"},inplace=True)


    bd["year"] = bd["Fecha"].dt.year
    bd["month"] = bd["Fecha"].dt.month
    
    return bd


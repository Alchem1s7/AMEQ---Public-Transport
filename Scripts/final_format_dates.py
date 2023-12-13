import pandas as pd


def formating(df):
        df.reset_index(drop=True,inplace=True)
        df["Fecha"] = df["Fecha"].astype(str)
        df["Fecha"] = df["Fecha"].str.replace("00:00:00","")
        df["Fecha"] = df["Fecha"].str.strip()
        return df
def every_row(row):
        lista = row.split("-")
        lista_invertida = lista[::-1]
        new_row = "-".join(lista_invertida)
        return new_row


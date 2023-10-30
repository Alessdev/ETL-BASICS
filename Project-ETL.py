"""
Python Extract Transform Load Example
"""

# %%
import requests
import pandas as pd
from sqlalchemy import create_engine

def extract()-> dict:
    """ Extraemos las universidades del Perú desde la API"""
    API_URL = "http://universities.hipolabs.com/search?country=Peru"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ Transformamos el dataset, mediante un filtro para asi obtener la universidades nacionales"""
    df = pd.DataFrame(data)
    print(f"El total de Universidades en el Perú, son: {len(data)}")
    df = df[df["name"].str.contains("Nacional")]
    print(f"El número de universidades Nacionales, son: {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]

def load(df:pd.DataFrame)-> None:
    """ Cargamos la data en un database sqllite"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')

# %%
data = extract()
df = transform(data)
load(df)

import requests
import pandas as pd
from datetime import datetime
import os

class DataExtractor:
    def __init__(self, url, payload, headers, querystring):
        self.url = url
        self.querystring = querystring
        self.headers = headers
        self.payload = payload

    def fetch_data(self):
        response = requests.request("GET", self.url, data=self.payload, headers=self.headers, params=self.querystring)
        response.raise_for_status()  # Lancar uma excecao se a requisicao nao for bem-sucedida
        return response.json()
    

    def process_data(self, data):
        lista = []
        for i in data['list']:
            lista.append(i)
        df= pd.json_normalize(lista)
        dtCarga = datetime.today().strftime('%m/%d/%y %H:%M')
        df['datacarga'] = dtCarga
        self.data = df
        return df
    
    def save_to_csv(self, df, path, filename):
        full_path = os.path.join(path, filename)
        df.to_csv(full_path, index=False, sep=';', decimal=',')



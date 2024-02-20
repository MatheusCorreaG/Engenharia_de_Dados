import pandas as pd

class Transform:
    def __init__(self, data):
        self.data = data

    def tipo_coluna(self, coluna, tipo):
        if coluna in self.data.columns:
            self.data[coluna] = self.data[coluna].astype(tipo)
        else:
            print(f"A coluna {coluna} não existe no DataFrame.")
       

    def fill_na(self, coluna, valor):
        self.data[coluna] = self.data[coluna].fillna(valor)
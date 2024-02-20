import pandas as pd
import numpy as np
import psycopg2 as sql
from sqlalchemy import create_engine, Column, Integer

class ConnDb:
    def __init__(self, host, database, port,  user, senha):
        self.host=host
        self.port=port
        self.database=database
        self.user=user
        self.senha=senha
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = sql.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.senha
            )
            self.cursor = self.connection.cursor()
            print("Conexao com o banco de dados estabelecida com sucesso.")
        except sql.Error as e:
            print(f"Erro na conexao com o banco de dados: {e}")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Conexao com o banco de dados encerrada.")
        

    def insertods(self, table, data):
        try:
            columns = ', '.join(data.columns)
            
            for index, row in data.iterrows():
                formatted_values = ', '.join([f"'{value}'" for value in row])
                query = f'INSERT INTO {table} ({columns}) VALUES ({formatted_values});'
                self.cursor.execute(query)
            
            self.connection.commit()
            print('Dados inseridos com sucesso')
        except sql.Error as e:
            print(f"Erro ao inserir dados: {e}")

    def insertdw(self, data):
        try:
            engine = create_engine(f"postgresql://{self.user}:{self.senha}@{self.host}:{self.port}/{self.database}")
            data.to_sql('tempacoes', engine, if_exists="replace", index=False)
            with engine.connect() as connection:
                connection.execute("ALTER TABLE tempacoes ADD COLUMN id SERIAL PRIMARY KEY;")
            
            qry_carga_incremental_acoes = '''
            SELECT a.companyid, a.companyname, a.ticker, a.price, a.p_l, a.p_vp, a.p_ebit, a.p_ativo, a.ev_ebit, a.margembruta, a.margemebit, a.margemliquida, a.p_sr, a.p_capitalgiro,	a.p_ativocirculante, a.giroativos,	a.roe, a.roa, a.roic, a.dividaliquidapatrimonioliquido,	a.dividaliquidaebit, a.pl_ativo, a.passivo_ativo, a.liquidezcorrente, a.peg_ratio, a.receitas_cagr5, a.liquidezmediadiaria, a.vpa, a.lpa, a.valormercado, a.segmentid, a.sectorid, a.subsectorid, a.subsectorname, a.segmentname, a.sectorname, a.dy, a.lucros_cagr5
                 FROM public."tempacoes" a
            LEFT JOIN acoes b 
            ON b.id = a.id
            WHERE b.id is null
            '''
            carga_acoes = pd.read_sql(qry_carga_incremental_acoes,self.connection)
            SQL_DW = self.connection.cursor()
            SQL_DW.executemany('''
            insert into acoes (companyid, companyname, ticker, price, p_l, p_vp, p_ebit, p_ativo, ev_ebit, margembruta, margemebit, margemliquida, p_sr, p_capitalgiro,	p_ativocirculante,	giroativos,	roe, roa, roic,	dividaliquidapatrimonioliquido,	dividaliquidaebit, pl_ativo, passivo_ativo, liquidezcorrente, peg_ratio, receitas_cagr5, liquidezmediadiaria, vpa, lpa, valormercado, segmentid, sectorid, subsectorid, subsectorname, segmentname, sectorname, dy, lucros_cagr5)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', carga_acoes.values.tolist())
            self.connection.commit()
            SQL_DW.execute('drop table public."tempacoes"')
            self.connection.commit()
            tamanho = len(carga_acoes)
            print(f"Dados inseridos com sucesso. {tamanho} valores inseridos")
        except sql.Error as e:
            print(f"Erro ao inserir dados: {e}")
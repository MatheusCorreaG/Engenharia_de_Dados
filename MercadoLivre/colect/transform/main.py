import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

df = pd.read_json('../../data/data.jsonl', lines=True)

# Setar o pandas para mostrar todas as colunas
pd.options.display.max_columns = None

df['source'] = "https://lista.mercadolivre.com.br/tenis-corrida-masculino"
df['data_coleta'] = datetime.now()

# Tratar os valores nulos para colunas numéricas e de texto
df['old_price_reais'] = df['old_price_reais'].fillna(0).astype(float)
df['old_price_centavos'] = df['old_price_centavos'].fillna(0).astype(float)
df['new_price_reais'] = df['new_price_reais'].fillna(0).astype(float)
df['new_price_centavos'] = df['new_price_centavos'].fillna(0).astype(float)
df['reviews_rating_number'] = df['reviews_rating_number'].fillna(0).astype(float)

# Remover os parênteses das colunas `reviews_amount`
df['reviews_amount'] = df['reviews_amount'].str.replace('[\(\)]', '', regex=True)
df['reviews_amount'] = df['reviews_amount'].fillna(0).astype(int)

# Tratar os preços como floats e calcular os valores totais
df['old_price'] = df['old_price_reais'] + df['old_price_centavos'] / 100
df['new_price'] = df['new_price_reais'] + df['new_price_centavos'] / 100

# Remover colunas antigas
df.drop(columns=['old_price_reais', 'old_price_centavos', 'new_price_reais', 'new_price_centavos'], inplace=True)

tabela = 'dadosml'

engine = create_engine('postgresql://postgres:bloqeiodofgh@localhost:5432/mercadolivre')
df.to_sql(tabela, con=engine, if_exists='replace')
engine.dispose() # Fechar conexão

print(df.head())
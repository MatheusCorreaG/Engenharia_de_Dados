from extractor import DataExtractor
from connector import ConnDb
from transformer import Transform

# Parametros para a instancia da classe DataExtractor
url = "https://statusinvest.com.br/category/advancedsearchresultpaginated"
querystring = {
    "search": "{\"Sector\":\"\",\"SubSector\":\"\",\"Segment\":\"\",\"my_range\":\"-20;100\",\"forecast\":{\"upsidedownside\":{\"Item1\":null,\"Item2\":null},\"estimatesnumber\":{\"Item1\":null,\"Item2\":null},\"revisedup\":true,\"reviseddown\":true,\"consensus\":[]},\"dy\":{\"Item1\":null,\"Item2\":null},\"p_l\":{\"Item1\":null,\"Item2\":null},\"peg_ratio\":{\"Item1\":null,\"Item2\":null},\"p_vp\":{\"Item1\":null,\"Item2\":null},\"p_ativo\":{\"Item1\":null,\"Item2\":null},\"margembruta\":{\"Item1\":null,\"Item2\":null},\"margemebit\":{\"Item1\":null,\"Item2\":null},\"margemliquida\":{\"Item1\":null,\"Item2\":null},\"p_ebit\":{\"Item1\":null,\"Item2\":null},\"ev_ebit\":{\"Item1\":null,\"Item2\":null},\"dividaliquidaebit\":{\"Item1\":null,\"Item2\":null},\"dividaliquidapatrimonioliquido\":{\"Item1\":null,\"Item2\":null},\"p_sr\":{\"Item1\":null,\"Item2\":null},\"p_capitalgiro\":{\"Item1\":null,\"Item2\":null},\"p_ativocirculante\":{\"Item1\":null,\"Item2\":null},\"roe\":{\"Item1\":null,\"Item2\":null},\"roic\":{\"Item1\":null,\"Item2\":null},\"roa\":{\"Item1\":null,\"Item2\":null},\"liquidezcorrente\":{\"Item1\":null,\"Item2\":null},\"pl_ativo\":{\"Item1\":null,\"Item2\":null},\"passivo_ativo\":{\"Item1\":null,\"Item2\":null},\"giroativos\":{\"Item1\":null,\"Item2\":null},\"receitas_cagr5\":{\"Item1\":null,\"Item2\":null},\"lucros_cagr5\":{\"Item1\":null,\"Item2\":null},\"liquidezmediadiaria\":{\"Item1\":null,\"Item2\":null},\"vpa\":{\"Item1\":null,\"Item2\":null},\"lpa\":{\"Item1\":null,\"Item2\":null},\"valormercado\":{\"Item1\":null,\"Item2\":null}}",
    "orderColumn": "",
    "isAsc": "",
    "page": "0",
    "take": "630",
    "CategoryType": "1"
}
payload = ""
headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "referer": "https://statusinvest.com.br/acoes/busca-avancada",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Linux; Android 11.0; Surface Duo) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
}

# Extracao dos dados, processamento e salvamento em CSV
caminho = 'dadosCsv'
extractor = DataExtractor(url, payload, headers, querystring) 
data = extractor.fetch_data()
processed_data = extractor.process_data(data)
extractor.save_to_csv(processed_data, caminho, 'acoes.csv')



# Transformação dos dados
alterar_colunas = Transform(processed_data)
alterar_colunas.fill_na('sectorid', 0)
alterar_colunas.fill_na('subsectorid', 0)
alterar_colunas.tipo_coluna('sectorid', int)
alterar_colunas.tipo_coluna('subsectorid', int)


# Banco de dados

# ODS
host = "localhost"
port = "5432"       
database = "acoesODS"  
user = "postgres"  
senha = "bloqeiodofgh" 

conexaoods = ConnDb(host, database, port,  user, senha)
conexaoods.connect()
conexaoods.insertods('logacoes', processed_data)
conexaoods.disconnect()

# DW
host = "localhost"
port = "5432"       
database = "acoesdw"  
user = "postgres"  
senha = "bloqeiodofgh" 

conexaodw = ConnDb(host, database, port,  user, senha)
conexaodw.connect()
conexaodw.insertdw(processed_data)
conexaodw.disconnect()

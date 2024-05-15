# Cria��o de um DataLake com dados em tempo real
 
## Descri��o
Este projeto tem como objetivo a cria��o de um Data Lake para simular o monitoramento de uma usina e�lica, com dados gerados em tempo real utilizando uma aplica��o em Python. A aplica��o gera dados referentes ao fator de pot�ncia, temperatura da bateria e press�o hidr�ulica.

Os registros, criados por diferentes scripts em Python, s�o extra�dos pelo Amazon Kinesis Data Streams e transmitidos para o Amazon S3 atrav�s do Amazon Kinesis Data Firehose. Ap�s a ingest�o, os dados s�o transformados para o formato Parquet e transportados para um outro bucket no Amazon S3. Finalmente, esses dados s�o analisados utilizando o Amazon Athena.

O objetivo do pipeline � obter uma an�lise eficiente e em tempo real dos dados operacionais de uma usina e�lica, demonstrando a capacidade de um Data Lake de gerenciar grandes volumes de dados de forma escal�vel e eficaz.
## Estrutura do reposit�rio
O reposit�rio est� organizado da seguinte forma:

scripts: Arquivos ipynb contendo os scripts de gera��o dos dados em tempo real

fluxograma: Cont�m o mapeamento do pipeline constru�do

dataset: Cont�m o dataset gerado pelo Amazon Athena ap�s consultar todos os dados gerados.

## Cr�ditos
Este projeto foi proposto pelo professor Fernando Amaral, atrav�s do curso forma��o Engenheiro de dados, no qual visa propor aos alunos uma imers�o de como um engenheiro de dados atua na pr�tica.
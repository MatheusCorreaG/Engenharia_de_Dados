# DataLake com dados em tempo real
 
## Descrição
Este projeto tem como objetivo a criação de um Data Lake para simular o monitoramento de uma usina eólica, com dados gerados em tempo real utilizando uma aplicação em Python. A aplicação gera dados referentes ao fator de potência, temperatura da bateria e pressão hidráulica.

Os registros, criados por diferentes scripts em Python, são extraídos pelo Amazon Kinesis Data Streams e transmitidos para o Amazon S3 através do Amazon Kinesis Data Firehose. Após a ingestão, os dados são transformados para o formato Parquet e transportados para um outro bucket no Amazon S3. Finalmente, esses dados são analisados utilizando o Amazon Athena.

O objetivo do pipeline é obter uma análise eficiente e em tempo real dos dados operacionais de uma usina eólica, demonstrando a capacidade de um Data Lake de gerenciar grandes volumes de dados de forma escalável e eficaz.

![fluxograma](https://github.com/MatheusCorreaG/Engenharia_de_Dados/assets/70293461/87a037ff-9449-42ea-b141-cb7815beee55)

## Estrutura do repositório
O repositório está organizado da seguinte forma:

scripts: Arquivos ipynb contendo os scripts de geração dos dados em tempo real

fluxograma: Contém o mapeamento do pipeline construído

dataset: Contém o dataset gerado pelo Amazon Athena após consultar todos os dados gerados.

## Créditos
Este projeto foi proposto pelo professor Fernando Amaral, através do curso formação Engenharia de Dados, no qual visa propor aos alunos uma imersão de como um engenheiro de dados atua na prática.

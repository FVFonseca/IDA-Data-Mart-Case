# BeAnalytics Case Python

Este projeto implementa um pipeline ETL (Extract, Transform, Load) para coletar, transformar e carregar dados do Índice de Desempenho no Atendimento (IDA) da ANATEL em um Data Mart PostgreSQL, utilizando Docker para orquestração dos serviços.

## Estrutura do Projeto
. ├── .gitignore 
├── docker-compose.yml 
├── README.md 
├── etl/ │ 
├── Dockerfile │
├── main.py │  
└── requirements.txt  
└── sql/
└── init.sql

## Componentes

- **[etl/main.py](etl/main.py)**: Script principal do pipeline ETL.
- **[etl/requirements.txt](etl/requirements.txt)**: Dependências Python do ETL.
- **[etl/Dockerfile](etl/Dockerfile)**: Dockerfile para o container do ETL.
- **[sql/init.sql](sql/init.sql)**: Script de criação do schema e tabelas no PostgreSQL.
- **[docker-compose.yml](docker-compose.yml)**: Orquestração dos containers do banco de dados e do ETL.

## Como Executar

1. **Clone o repositório**  
   ```sh
   git clone <url-do-repositorio>
   cd BeAnalytics-Case-Python

2. **Suba os containers com Docker Compose**
   ```sh
   docker-compose up --build
   ```

Isso irá:

Inicializar um container PostgreSQL com o schema e tabelas definidos em init.sql.
Construir e executar o container do ETL, que irá extrair, transformar e carregar os dados para o banco.

3. **Acompanhe os logs**
Os logs do ETL e do banco podem ser visualizados no terminal ou com:
    ``` docker logs ida_etl
docker logs ida_postgres ```

4. **Verifique os dados no PostgreSQL**
   Você pode acessar o banco de dados PostgreSQL usando um cliente SQL ou via terminal:
   ```sh
   docker exec -it ida_postgres psql -U postgres -d ida_db
   ```

## Configuração de Ambiente
As variáveis de ambiente para conexão com o banco são definidas no docker-compose.yml. Por exemplo:

- DB_HOST
- DB_NAME
- DB_USER
- DB_PASSWORD

## Dependências
As dependências Python estão listadas em requirements.txt:

- pandas
- requests
- psycopg2-binary
- sqlalchemy
- odfpy

## Observações
- O script ETL baixa arquivos ODS diretamente do portal da ANATEL.

- O banco de dados é inicializado com tabelas e views para análise dos dados de desempenho.

import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from io import StringIO, BytesIO 
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataExtractor:
    """
    Classe responsável por extrair dados do portal de Dados Abertos da ANATEL.
    """
    BASE_URL = "https://dados.gov.br/dados/conjuntos-dados/indice-desempenho-atendimento"
    FILES = {
         "SMP": "https://www.anatel.gov.br/dadosabertos/PDA/IDA/SMP2019.ods",
        "STFC": "https://www.anatel.gov.br/dadosabertos/PDA/IDA/STFC2019.ods",
        "SCM": "https://www.anatel.gov.br/dadosabertos/PDA/IDA/SCM2019.ods"
    }

    def __init__(self):
        logging.info("Inicializando DataExtractor.")

    def download_data(self, service_name: str) -> pd.DataFrame:
        """
        Baixa os dados do serviço especificado (CSV ou ODS) e retorna um DataFrame.

        Args:
            service_name (str): Nome do serviço (SMP, STFC, SCM).

        Returns:
            pd.DataFrame: DataFrame contendo os dados baixados.
        """
        if service_name not in self.FILES:
            logging.error(f"Serviço '{service_name}' não encontrado.")
            raise ValueError(f"Serviço '{service_name}' não suportado.")

        url = self.FILES[service_name]
        logging.info(f"Baixando dados para {service_name} da URL: {url}")
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()

            # Detectar a extensão do arquivo para decidir como ler
            if url.lower().endswith('.csv'):
                content = response.content.decode('utf-8')
                df = pd.read_csv(StringIO(content), sep=';')
                logging.info(f"Dados CSV para {service_name} baixados e lidos com sucesso. Linhas: {len(df)}")
            elif url.lower().endswith('.ods'):
                # Para ODS, leia o conteúdo binário e use pd.read_excel
                # É crucial ter 'odfpy' ou 'calamine' instalado
                df = pd.read_excel(BytesIO(response.content), engine='odf') # Ou 'calamine'
                logging.info(f"Dados ODS para {service_name} baixados e lidos com sucesso. Linhas: {len(df)}")
            else:
                logging.error(f"Formato de arquivo não suportado para a URL: {url}")
                raise ValueError("Formato de arquivo não suportado.")

            return df
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao baixar dados para {service_name}: {e}")
            raise
        except pd.errors.EmptyDataError:
            logging.warning(f"O arquivo baixado para {service_name} está vazio.")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Erro inesperado ao processar dados para {service_name}: {e}")
            raise

class DataTransformer:
    """
    Classe responsável por transformar os dados brutos em um formato adequado para o Data Mart.
    """
    def __init__(self):
        logging.info("Inicializando DataTransformer.")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Realiza as transformações nos dados brutos.

        Args:
            df (pd.DataFrame): DataFrame contendo os dados brutos.

        Returns:
            pd.DataFrame: DataFrame com os dados transformados.
        """
        if df.empty:
            logging.warning("DataFrame de entrada está vazio. Nenhuma transformação será realizada.")
            return pd.DataFrame()

        logging.info("Iniciando transformação dos dados.")
        
        # Renomear colunas para corresponder ao Data Mart
        df = df.rename(columns={
            'Ano': 'Ano',
            'Mês': 'Mês',
            'Grupo Econômico': 'Grupo Econômico',
            'Índice de Desempenho no Atendimento': 'Taxa de Resolvidas em 5 dias úteis',
            'Total de Demandas': 'Total de Demandas',
            'Total Resolvidas': 'Total Resolvidas',
            'Total Não Resolvidas': 'Total Não Resolvidas'
        })

        # Selecionar apenas as colunas de interesse
        columns_to_keep = [
            'Ano', 'Mês', 'Grupo Econômico',
            'Taxa de Resolvidas em 5 dias úteis',
            'Total de Demandas', 'Total Resolvidas', 'Total Não Resolvidas'
        ]
        df = df[columns_to_keep]

        # Converter tipos de dados
        # Converter colunas numéricas, tratando valores não numéricos como NaN
        for col in ['Taxa de Resolvidas em 5 dias úteis', 'Total de Demandas', 'Total Resolvidas', 'Total Não Resolvidas']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Remover linhas com valores nulos nas colunas críticas
        df.dropna(subset=['Ano', 'Mês', 'Grupo Econômico', 'Taxa de Resolvidas em 5 dias úteis'], inplace=True)
        
        # Converter Ano e Mês para int
        df['Ano'] = df['Ano'].astype(int)
        df['Mês'] = df['Mês'].astype(int)
        
        logging.info("Transformação dos dados concluída.")
        return df

class DataLoader:
    """
    Classe responsável por carregar os dados transformados no banco PostgreSQL.
    """
    def __init__(self, host, db, user, password):
        self.conn_str = (
            f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
        )
        self.engine = create_engine(self.conn_str)
        logging.info(f"Conectado ao banco de dados: {db} no host: {host}")

    def load_data(self, df: pd.DataFrame, service_name: str):
        """
        Carrega os dados transformados na tabela fato_atendimento.
        Args:
            df (pd.DataFrame): DataFrame com os dados transformados.
            service_name (str): Nome do serviço (SMP, STFC, SCM).
        """
        if df.empty:
            logging.warning(f"Nenhum dado para carregar para o serviço {service_name}.")
            return

        # Adiciona coluna de serviço
        df['Servico'] = service_name

        # Define o nome da tabela e o schema
        table_name = 'fato_atendimento'
        schema = 'ida_datamart'

        try:
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists='append',
                index=False,
                method='multi'
            )
            logging.info(f"{len(df)} linhas carregadas na tabela {schema}.{table_name} para o serviço {service_name}.")
        except Exception as e:
            logging.error(f"Erro ao carregar dados para {service_name}: {e}")
            raise

def main():
    """
    Função principal que orquestra o processo ETL.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "ida_datamart")
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")

    extractor = DataExtractor()
    transformer = DataTransformer()
    loader = DataLoader(db_host, db_name, db_user, db_password)

    services = ["SMP", "STFC", "SCM"]

    for service in services:
        try:
            logging.info(f"Iniciando ETL para o serviço: {service}")
            raw_df = extractor.download_data(service)
            transformed_df = transformer.transform(raw_df)
            loader.load_data(transformed_df, service)
            logging.info(f"ETL para o serviço {service} concluído com sucesso.")
        except Exception as e:
            logging.error(f"Falha no processo ETL para o serviço {service}: {e}")

if __name__ == "__main__":
    logging.info("Iniciando script ETL.")
    main()
    logging.info("Script ETL finalizado.")



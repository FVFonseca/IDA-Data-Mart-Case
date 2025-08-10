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
        if service_name not in self.FILES:
            logging.error(f"Serviço '{service_name}' não encontrado.")
            raise ValueError(f"Serviço '{service_name}' não suportado.")

        url = self.FILES[service_name]
        logging.info(f"Baixando dados para {service_name} da URL: {url}")
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status() 

            if url.lower().endswith('.csv'):
                content = response.content.decode('utf-8')
                df = pd.read_csv(StringIO(content), sep=';')
                logging.info(f"Dados CSV para {service_name} baixados e lidos com sucesso. Linhas: {len(df)}")
                return df
            elif url.lower().endswith('.ods'):
                df = pd.read_excel(BytesIO(response.content), engine='odf', skiprows=8)
                logging.info(f"Dados ODS para {service_name} baixados e lidos com sucesso. Linhas: {len(df)}")
                return df
            else:
                logging.error(f"Formato de arquivo não suportado para a URL: {url}")
                raise ValueError("Formato de arquivo não suportado. A lógica atual suporta apenas .ods e .csv.")
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
    Classe responsável por transformar os dados brutos em um formato adequado para o Data Mart,
    baseado na estrutura exata da imagem fornecida.
    """
    def __init__(self):
        logging.info("Inicializando DataTransformer.")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            logging.warning("DataFrame de entrada está vazio.")
            return pd.DataFrame()

        logging.info("Iniciando transformação dos dados.")

        df = df.rename(columns={
            'GRUPO ECONÔMICO': 'Grupo Econômico',
            'VARIÁVEL': 'Variavel'
        })

        id_vars = ['Grupo Econômico', 'Variavel']
        if not all(col in df.columns for col in id_vars):
            raise ValueError(f"Colunas de identificação não encontradas após renomeação. Disponíveis: {df.columns.tolist()}")

        value_vars = [col for col in df.columns if isinstance(col, str) and col.startswith('20')]
        for col in value_vars:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df_melted = df.melt(id_vars=id_vars, var_name='Data_Mes_Original', value_name='Valor')

        df_melted['Data'] = pd.to_datetime(df_melted['Data_Mes_Original'], format='%Y-%m', errors='coerce')
        df_melted.dropna(subset=['Data'], inplace=True)
        df_melted['Ano'] = df_melted['Data'].dt.year
        df_melted['Mês'] = df_melted['Data'].dt.month
        
        df_melted.dropna(subset=['Variavel', 'Grupo Econômico', 'Ano', 'Mês'], inplace=True)
        df_final = df_melted.pivot_table(
            index=['Ano', 'Mês', 'Grupo Econômico'],
            columns='Variavel',
            values='Valor',
            aggfunc='first'
        ).reset_index()

 
        df_final = df_final.rename(columns={
            'Indicador de Desempenho no Atendimento (IDA)': 'indicador_desempenho_atendimento',
            'Índice de Reclamações': 'indice_reclamacoes',
            'Quantidade de acessos em serviço': 'quantidade_acessos_servico',
            'Quantidade de reabertas': 'quantidade_reabertas',
            'Quantidade de reclamações': 'quantidade_reclamacoes',
            'Quantidade de reclamações no Período': 'quantidade_reclamacoes_periodo',
            'Quantidade de Respondidas': 'quantidade_respondidas',
            'Quantidade de Sol. Respondidas em até 5 dias': 'quantidade_sol_respondidas_5_dias',
            'Quantidade de Sol. Respondidas no Período': 'quantidade_sol_respondidas_periodo',
            'Taxa de Reabertas': 'taxa_reabertas',
            'Taxa de Respondidas em 5 dias Úteis': 'taxa_respondidas_5_dias_uteis',
            'Taxa de Respondidas no Período': 'taxa_respondidas_periodo'
        })

        metric_columns = [
            'indicador_desempenho_atendimento', 'indice_reclamacoes', 'quantidade_acessos_servico',
            'quantidade_reabertas', 'quantidade_reclamacoes', 'quantidade_reclamacoes_periodo',
            'quantidade_respondidas', 'quantidade_sol_respondidas_5_dias', 'quantidade_sol_respondidas_periodo',
            'taxa_reabertas', 'taxa_respondidas_5_dias_uteis', 'taxa_respondidas_periodo'
        ]
        
        for col_name in metric_columns:
            if col_name not in df_final.columns:
                df_final[col_name] = pd.NA # Adiciona coluna se não existir
            df_final[col_name] = pd.to_numeric(df_final[col_name], errors='coerce')

        df_final.dropna(subset=['indicador_desempenho_atendimento'], inplace=True)
        
        final_columns = ['Ano', 'Mês', 'Grupo Econômico'] + metric_columns
        df_final = df_final[final_columns]
        
        logging.info(f"Transformação dos dados concluída. DataFrame final com {len(df_final)} linhas.")
        return df_final



from sqlalchemy.orm import sessionmaker

from sqlalchemy.orm import sessionmaker

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

    def _insert_or_get_id(self, table_name: str, pk_column: str, lookup_column: str, value: any, extra_params: dict = None) -> int:
        """
        Insere um valor em uma tabela de dimensão ou retorna seu ID se já existir.
        """
        with self.engine.connect() as conn:
            if table_name == 'dim_tempo':
                query_select = text(f"SELECT {pk_column} FROM ida_datamart.dim_tempo WHERE ano=:ano AND mes=:mes")
                result = conn.execute(query_select, {"ano": extra_params['ano'], "mes": extra_params['mes']}).scalar_one_or_none()
                if result:
                    return result
                else:
                    query_insert = text(f"INSERT INTO ida_datamart.dim_tempo (ano, mes, data_completa) VALUES (:ano, :mes, make_date(:ano, :mes, 1)) RETURNING {pk_column}")
                    new_id = conn.execute(query_insert, {"ano": extra_params['ano'], "mes": extra_params['mes']}).scalar_one()
            else:
                query_select = text(f"SELECT {pk_column} FROM ida_datamart.{table_name} WHERE {lookup_column} ILIKE :value")
                result = conn.execute(query_select, {'value': value}).scalar_one_or_none()
                if result:
                    return result
                else:
                    query_insert = text(f"INSERT INTO ida_datamart.{table_name} ({lookup_column}) VALUES (:value) RETURNING {pk_column}")
                    new_id = conn.execute(query_insert, {'value': value}).scalar_one()

            conn.commit()
            return new_id

    def load_data(self, df: pd.DataFrame, service_name: str):
        """
        Carrega os dados transformados na tabela fato, resolvendo os IDs das dimensões.
        """
        if df.empty:
            logging.warning(f"DataFrame vazio para o serviço {service_name}. Nada para carregar.")
            return

        logging.info(f"Iniciando carga de dados para o serviço: {service_name}. Total de linhas a processar: {len(df)}")


        df.columns = [col.lower() for col in df.columns]
        df = df.rename(columns={
            'mês': 'mes',  
            'grupo econômico': 'grupo_economico'
        })
        
        records_for_fact = []
        id_servico = self._insert_or_get_id('dim_servico', 'id_servico', 'nome_servico', f"%{service_name}%")

        for index, row in df.iterrows():
            try:
                id_tempo = self._insert_or_get_id('dim_tempo', 'id_tempo', None, None, {'ano': row['ano'], 'mes': row['mes']})
                id_grupo_economico = self._insert_or_get_id('dim_grupo_economico', 'id_grupo_economico', 'nome_grupo_economico', row['grupo_economico'])
                
                record = row.to_dict()
                record['id_tempo'] = id_tempo
                record['id_grupo_economico'] = id_grupo_economico
                record['id_servico'] = id_servico
                records_for_fact.append(record)

            except Exception as e:
                logging.error(f"Erro ao processar linha {index} para carga: {e}. Dados: {row.to_dict()}")
                continue
        
        if not records_for_fact:
            logging.warning(f"Nenhum registro válido para carregar na tabela fato para o serviço {service_name}.")
            return

        fact_df_to_load = pd.DataFrame(records_for_fact)

        db_columns = [
            'id_tempo', 'id_servico', 'id_grupo_economico', 'indicador_desempenho_atendimento',
            'indice_reclamacoes', 'quantidade_acessos_servico', 'quantidade_reabertas',
            'quantidade_reclamacoes', 'quantidade_reclamacoes_periodo', 'quantidade_respondidas',
            'quantidade_sol_respondidas_5_dias', 'quantidade_sol_respondidas_periodo',
            'taxa_reabertas', 'taxa_respondidas_5_dias_uteis', 'taxa_respondidas_periodo'
        ]
        fact_df_to_load = fact_df_to_load[db_columns]

        try:
            fact_df_to_load.to_sql(
                'fato_desempenho_atendimento',
                self.engine,
                schema='ida_datamart',
                if_exists='append',
                index=False
            )
            logging.info(f"SUCESSO! {len(fact_df_to_load)} linhas carregadas na tabela para o serviço {service_name}.")
        except Exception as e:
            logging.error(f"Erro fatal ao carregar dados na tabela fato para {service_name}: {e}")
            raise


def main():
    """
    Função principal que orquestra o processo ETL.
    """
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME", "ida_datamart")
    db_user = os.getenv("DB_USER", "user")
    db_password = os.getenv("DB_PASSWORD", "password")

    if not all([db_host, db_name, db_user, db_password]):
        logging.error("Variáveis de ambiente para conexão com o banco de dados não estão configuradas corretamente. Verifique DB_HOST, DB_NAME, DB_USER, DB_PASSWORD.")
        raise ValueError("Configuração de conexão com o banco de dados está incompleta.")

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

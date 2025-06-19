-- Criação do esquema para o Data Mart
CREATE SCHEMA IF NOT EXISTS ida_datamart;

SET search_path TO ida_datamart;

-- Tabela de Dimensão: Tempo
CREATE TABLE IF NOT EXISTS dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    ano INT NOT NULL,
    mes INT NOT NULL,
    data_completa DATE NOT NULL UNIQUE
);

COMMENT ON TABLE dim_tempo IS 'Dimensão de tempo para o Data Mart de Desempenho no Atendimento.';
COMMENT ON COLUMN dim_tempo.id_tempo IS 'Chave primária da dimensão de tempo.';
COMMENT ON COLUMN dim_tempo.ano IS 'Ano da ocorrência.';
COMMENT ON COLUMN dim_tempo.mes IS 'Mês da ocorrência.';
COMMENT ON COLUMN dim_tempo.data_completa IS 'Data completa da ocorrência (primeiro dia do mês).';

-- Tabela de Dimensão: Serviço
CREATE TABLE IF NOT EXISTS dim_servico (
    id_servico SERIAL PRIMARY KEY,
    nome_servico VARCHAR(50) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_servico IS 'Dimensão de serviço para o Data Mart de Desempenho no Atendimento.';
COMMENT ON COLUMN dim_servico.id_servico IS 'Chave primária da dimensão de serviço.';
COMMENT ON COLUMN dim_servico.nome_servico IS 'Nome do serviço (SMP, STFC, SCM).';

-- Tabela de Dimensão: Grupo Econômico
CREATE TABLE IF NOT EXISTS dim_grupo_economico (
    id_grupo_economico SERIAL PRIMARY KEY,
    nome_grupo_economico VARCHAR(100) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_grupo_economico IS 'Dimensão de grupo econômico para o Data Mart de Desempenho no Atendimento.';
COMMENT ON COLUMN dim_grupo_economico.id_grupo_economico IS 'Chave primária da dimensão de grupo econômico.';
COMMENT ON COLUMN dim_grupo_economico.nome_grupo_economico IS 'Nome do grupo econômico.';

-- Tabela Fato: Desempenho de Atendimento
CREATE TABLE IF NOT EXISTS fato_desempenho_atendimento (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL,
    id_servico INTEGER NOT NULL,
    id_grupo_economico INTEGER NOT NULL,
    taxa_resolvidas_5_dias_uteis NUMERIC(5,2),
    total_demandas INTEGER,
    total_resolvidas INTEGER,
    total_nao_resolvidas INTEGER,
    FOREIGN KEY (id_tempo) REFERENCES dim_tempo (id_tempo),
    FOREIGN KEY (id_servico) REFERENCES dim_servico (id_servico),
    FOREIGN KEY (id_grupo_economico) REFERENCES dim_grupo_economico (id_grupo_economico)
);

COMMENT ON TABLE fato_desempenho_atendimento IS 'Tabela fato com os dados de desempenho no atendimento.';
COMMENT ON COLUMN fato_desempenho_atendimento.id_fato IS 'Chave primária da tabela fato.';
COMMENT ON COLUMN fato_desempenho_atendimento.id_tempo IS 'Chave estrangeira para a dimensão de tempo.';
COMMENT ON COLUMN fato_desempenho_atendimento.id_servico IS 'Chave estrangeira para a dimensão de serviço.';
COMMENT ON COLUMN fato_desempenho_atendimento.id_grupo_economico IS 'Chave estrangeira para a dimensão de grupo econômico.';
COMMENT ON COLUMN fato_desempenho_atendimento.taxa_resolvidas_5_dias_uteis IS 'Taxa de demandas resolvidas em 5 dias úteis.';
COMMENT ON COLUMN fato_desempenho_atendimento.total_demandas IS 'Número total de demandas.';
COMMENT ON COLUMN fato_desempenho_atendimento.total_resolvidas IS 'Número total de demandas resolvidas.';
COMMENT ON COLUMN fato_desempenho_atendimento.total_nao_resolvidas IS 'Número total de demandas não resolvidas.';

-- Inserção de dados nas tabelas de dimensão, se ainda não existirem
INSERT INTO dim_servico (nome_servico) VALUES
('Serviço Móvel Pessoal – SMP'),
('Serviço Telefônico Fixo Comutado – STFC'),
('Serviço de Comunicação Multimídia – SCM')
ON CONFLICT (nome_servico) DO NOTHING;


-- View para a taxa de variação e a diferença entre a taxa de variação média e individual
CREATE OR REPLACE VIEW ida_taxa_variacao_pivotada AS
WITH ida_mensal AS (
    SELECT
        dt.data_completa AS mes,
        dge.nome_grupo_economico AS grupo_economico,
        AVG(fda.taxa_resolvidas_5_dias_uteis) AS ida_mensal_medio
    FROM
        ida_datamart.fato_desempenho_atendimento fda
    JOIN
        ida_datamart.dim_tempo dt ON fda.id_tempo = dt.id_tempo
    JOIN
        ida_datamart.dim_grupo_economico dge ON fda.id_grupo_economico = dge.id_grupo_economico
    GROUP BY
        dt.data_completa, dge.nome_grupo_economico
),
taxa_variacao AS (
    SELECT
        mes,
        grupo_economico,
        ida_mensal_medio,
        LAG(ida_mensal_medio, 1, 0) OVER (PARTITION BY grupo_economico ORDER BY mes) AS ida_mes_anterior,
        CASE
            WHEN LAG(ida_mensal_medio, 1, 0) OVER (PARTITION BY grupo_economico ORDER BY mes) = 0 THEN NULL -- Evita divisão por zero
            ELSE ((ida_mensal_medio - LAG(ida_mensal_medio, 1, 0) OVER (PARTITION BY grupo_economico ORDER BY mes)) / LAG(ida_mensal_medio, 1, 0) OVER (PARTITION BY grupo_economico ORDER BY mes)) * 100
        END AS taxa_variacao_individual
    FROM
        ida_mensal
),
taxa_variacao_media AS (
    SELECT
        mes,
        AVG(taxa_variacao_individual) AS taxa_variacao_media
    FROM
        taxa_variacao
    GROUP BY
        mes
),
pivot_grupos AS (
    SELECT
        tv.mes,
        tvm.taxa_variacao_media,
        tv.grupo_economico,
        tv.taxa_variacao_individual,
        (tvm.taxa_variacao_media - tv.taxa_variacao_individual) AS diferenca_da_media
    FROM
        taxa_variacao tv
    JOIN
        taxa_variacao_media tvm ON tv.mes = tvm.mes
)
SELECT
    p.mes,
    p.taxa_variacao_media,
    MAX(CASE WHEN p.grupo_economico = 'ALGAR' THEN p.diferenca_da_media END) AS "ALGAR",
    MAX(CASE WHEN p.grupo_economico = 'CLARO' THEN p.diferenca_da_media END) AS "CLARO",
    MAX(CASE WHEN p.grupo_economico = 'OI' THEN p.diferenca_da_media END) AS "OI",
    MAX(CASE WHEN p.grupo_economico = 'TIM' THEN p.diferenca_da_media END) AS "TIM",
    MAX(CASE WHEN p.grupo_economico = 'VIVO' THEN p.diferenca_da_media END) AS "VIVO",
    MAX(CASE WHEN p.grupo_economico = 'NEXTEL' THEN p.diferenca_da_media END) AS "NEXTEL",
    MAX(CASE WHEN p.grupo_economico = 'SKY' THEN p.diferenca_da_media END) AS "SKY"
FROM
    pivot_grupos p
GROUP BY
    p.mes, p.taxa_variacao_media
ORDER BY
    p.mes;

COMMENT ON VIEW ida_datamart.ida_taxa_variacao_pivotada IS 'View que calcula a taxa de variação mensal da "Taxa de Resolvidas em 5 dias úteis" para cada grupo econômico e a diferença em relação à taxa de variação média, com os grupos econômicos pivotados.';



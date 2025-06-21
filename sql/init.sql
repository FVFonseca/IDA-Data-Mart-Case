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

-- Tabela de Dimensão: Serviço
CREATE TABLE IF NOT EXISTS dim_servico (
    id_servico SERIAL PRIMARY KEY,
    nome_servico VARCHAR(50) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_servico IS 'Dimensão de serviço para o Data Mart de Desempenho no Atendimento.';

-- Tabela de Dimensão: Grupo Econômico
CREATE TABLE IF NOT EXISTS dim_grupo_economico (
    id_grupo_economico SERIAL PRIMARY KEY,
    nome_grupo_economico VARCHAR(100) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_grupo_economico IS 'Dimensão de grupo econômico para o Data Mart de Desempenho no Atendimento.';

-- Tabela Fato: Desempenho de Atendimento
-- CORREÇÃO: Removido o hífen inicial e adicionado "IF NOT EXISTS" para robustez.
CREATE TABLE IF NOT EXISTS fato_desempenho_atendimento (
    id_fato SERIAL PRIMARY KEY,
    id_tempo INTEGER NOT NULL,
    id_servico INTEGER NOT NULL,
    id_grupo_economico INTEGER NOT NULL,
    indicador_desempenho_atendimento NUMERIC(10, 4),
    indice_reclamacoes NUMERIC(10, 4),
    quantidade_acessos_servico NUMERIC(15, 0),
    quantidade_reabertas NUMERIC(15, 0),
    quantidade_reclamacoes NUMERIC(15, 0),
    quantidade_reclamacoes_periodo NUMERIC(15, 0),
    quantidade_respondidas NUMERIC(15, 0),
    quantidade_sol_respondidas_5_dias NUMERIC(15, 0),
    quantidade_sol_respondidas_periodo NUMERIC(15, 0),
    taxa_reabertas NUMERIC(10, 4),
    taxa_respondidas_5_dias_uteis NUMERIC(10, 4),
    taxa_respondidas_periodo NUMERIC(10, 4),
    FOREIGN KEY (id_tempo) REFERENCES dim_tempo (id_tempo),
    FOREIGN KEY (id_servico) REFERENCES dim_servico (id_servico),
    FOREIGN KEY (id_grupo_economico) REFERENCES dim_grupo_economico (id_grupo_economico)
);

COMMENT ON TABLE fato_desempenho_atendimento IS 'Tabela fato com os dados de desempenho no atendimento, com todas as métricas da fonte.';
COMMENT ON COLUMN fato_desempenho_atendimento.indicador_desempenho_atendimento IS 'Indicador de Desempenho no Atendimento (IDA).';
COMMENT ON COLUMN fato_desempenho_atendimento.indice_reclamacoes IS 'Índice de Reclamações.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_acessos_servico IS 'Quantidade de acessos em serviço.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_reabertas IS 'Quantidade de solicitações reabertas.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_reclamacoes IS 'Quantidade de reclamações recebidas.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_reclamacoes_periodo IS 'Quantidade de reclamações no período.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_respondidas IS 'Quantidade de solicitações respondidas.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_sol_respondidas_5_dias IS 'Quantidade de solicitações respondidas em até 5 dias.';
COMMENT ON COLUMN fato_desempenho_atendimento.quantidade_sol_respondidas_periodo IS 'Quantidade de solicitações respondidas no período.';
COMMENT ON COLUMN fato_desempenho_atendimento.taxa_reabertas IS 'Taxa de Reabertas.';
COMMENT ON COLUMN fato_desempenho_atendimento.taxa_respondidas_5_dias_uteis IS 'Taxa de Respondidas em 5 dias Úteis.';
COMMENT ON COLUMN fato_desempenho_atendimento.taxa_respondidas_periodo IS 'Taxa de Respondidas no Período.';


-- View para a taxa de variação e a diferença entre a taxa de variação média e individual
CREATE OR REPLACE VIEW ida_taxa_variacao_pivotada AS
WITH ida_mensal AS (
    SELECT
        dt.data_completa AS mes,
        dge.nome_grupo_economico AS grupo_economico,
        AVG(fda.taxa_respondidas_5_dias_uteis) AS ida_mensal_medio
    FROM
        ida_datamart.fato_desempenho_atendimento fda
    JOIN
        ida_datamart.dim_tempo dt ON fda.id_tempo = dt.id_tempo
    JOIN
        ida_datamart.dim_grupo_economico dge ON fda.id_grupo_economico = dge.id_grupo_economico
    WHERE
        fda.taxa_respondidas_5_dias_uteis IS NOT NULL
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
            WHEN LAG(ida_mensal_medio, 1, 0) OVER (PARTITION BY grupo_economico ORDER BY mes) = 0 THEN NULL
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
    MAX(CASE WHEN p.grupo_economico = 'SKY' THEN p.diferenca_da_media END) AS "SKY",
    MAX(CASE WHEN p.grupo_economico = 'SERCOMTEL' THEN p.diferenca_da_media END) AS "SERCOMTEL"
FROM
    pivot_grupos p
GROUP BY
    p.mes, p.taxa_variacao_media
ORDER BY
    p.mes;
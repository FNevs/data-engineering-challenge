-- Camada refined: agregacoes que respondem as questoes do desafio.
--
-- Cada tabela aqui corresponde a uma das quatro perguntas. Materializar as
-- respostas em vez de deixar apenas as queries soltas tem uma razao pratica:
-- a consulta de quartis sobre ~38M linhas custa caro, e materializar o
-- resultado torna a consulta final instantanea.

-- ---------------------------------------------------------------------------
-- Questao 1: total de registros na tabela final
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS refined.resumo_volumetria;

CREATE TABLE refined.resumo_volumetria AS
SELECT
    (SELECT COUNT(*) FROM raw.yellow_tripdata)     AS total_registros_raw,
    (SELECT COUNT(*) FROM trusted.viagens)         AS total_registros_trusted,
    (SELECT COUNT(*) FROM raw.yellow_tripdata)
        - (SELECT COUNT(*) FROM trusted.viagens)   AS total_descartado,
    ROUND(
        100.0 * ((SELECT COUNT(*) FROM raw.yellow_tripdata)
                 - (SELECT COUNT(*) FROM trusted.viagens))
        / NULLIF((SELECT COUNT(*) FROM raw.yellow_tripdata), 0),
        4
    )                                              AS percentual_descartado,
    NOW()                                          AS calculado_em;

COMMENT ON TABLE refined.resumo_volumetria IS
    'Questao 1: volumetria das camadas e taxa de descarte na limpeza.';


-- ---------------------------------------------------------------------------
-- Questao 2: viagens iniciadas e finalizadas em 17 de junho
--
-- O enunciado nao especifica o ano, mas o dataset cobre apenas 2022, entao a
-- data e 2022-06-17. A pergunta tambem admite duas leituras: "iniciadas e
-- finalizadas no mesmo dia 17" ou "iniciadas no dia 17" e "finalizadas no dia
-- 17" como numeros separados. As tres metricas sao calculadas para que a
-- resposta cubra qualquer interpretacao.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS refined.viagens_17_junho;

CREATE TABLE refined.viagens_17_junho AS
SELECT
    DATE '2022-06-17'                                        AS data_referencia,
    COUNT(*) FILTER (
        WHERE data_inicio = DATE '2022-06-17'
    )                                                        AS viagens_iniciadas,
    COUNT(*) FILTER (
        WHERE data_fim = DATE '2022-06-17'
    )                                                        AS viagens_finalizadas,
    COUNT(*) FILTER (
        WHERE data_inicio = DATE '2022-06-17'
          AND data_fim = DATE '2022-06-17'
    )                                        AS iniciadas_e_finalizadas_no_dia,
    NOW()                                                    AS calculado_em
FROM trusted.viagens
WHERE data_inicio = DATE '2022-06-17'
   OR data_fim = DATE '2022-06-17';

COMMENT ON TABLE refined.viagens_17_junho IS
    'Questao 2: contagem de viagens em 17/06/2022 sob tres interpretacoes.';


-- ---------------------------------------------------------------------------
-- Questao 3: dia da viagem mais longa percorrida
--
-- "Mais longa" e ambiguo entre distancia e duracao. A leitura principal e
-- distancia, pelo contexto da questao 4, mas a duracao tambem e materializada.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS refined.viagem_mais_longa;

CREATE TABLE refined.viagem_mais_longa AS
WITH maior_distancia AS (
    SELECT
        'distancia'::TEXT AS criterio,
        viagem_id,
        data_inicio,
        datahora_inicio,
        datahora_fim,
        distancia_milhas,
        distancia_km,
        duracao_minutos,
        valor_total
    FROM trusted.viagens
    ORDER BY distancia_milhas DESC
    LIMIT 1
),
maior_duracao AS (
    SELECT
        'duracao'::TEXT AS criterio,
        viagem_id,
        data_inicio,
        datahora_inicio,
        datahora_fim,
        distancia_milhas,
        distancia_km,
        duracao_minutos,
        valor_total
    FROM trusted.viagens
    ORDER BY duracao_minutos DESC
    LIMIT 1
)
SELECT * FROM maior_distancia
UNION ALL
SELECT * FROM maior_duracao;

COMMENT ON TABLE refined.viagem_mais_longa IS
    'Questao 3: viagem mais longa por distancia e por duracao.';


-- ---------------------------------------------------------------------------
-- Questao 4: estatisticas descritivas da distancia percorrida
--
-- PERCENTILE_CONT interpola entre valores adjacentes (definicao continua de
-- quartil, igual ao padrao do pandas.describe()), o que torna o resultado
-- comparavel a uma analise exploratoria feita em Python.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS refined.estatisticas_distancia;

CREATE TABLE refined.estatisticas_distancia AS
SELECT
    COUNT(*)                                              AS qtd_viagens,
    ROUND(AVG(distancia_milhas), 4)                       AS media_milhas,
    ROUND(STDDEV_SAMP(distancia_milhas), 4)               AS desvio_padrao_milhas,
    MIN(distancia_milhas)                                 AS minimo_milhas,
    ROUND(
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY distancia_milhas)::NUMERIC,
        4
    )                                                     AS q1_milhas,
    ROUND(
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY distancia_milhas)::NUMERIC,
        4
    )                                                     AS mediana_milhas,
    ROUND(
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY distancia_milhas)::NUMERIC,
        4
    )                                                     AS q3_milhas,
    MAX(distancia_milhas)                                 AS maximo_milhas,
    -- Mesmas metricas em quilometros, para leitura direta.
    ROUND(AVG(distancia_km), 4)                           AS media_km,
    ROUND(STDDEV_SAMP(distancia_km), 4)                   AS desvio_padrao_km,
    MIN(distancia_km)                                     AS minimo_km,
    MAX(distancia_km)                                     AS maximo_km,
    NOW()                                                 AS calculado_em
FROM trusted.viagens;

COMMENT ON TABLE refined.estatisticas_distancia IS
    'Questao 4: media, desvio padrao, minimo, maximo e quartis da distancia.';


-- ---------------------------------------------------------------------------
-- Tabela de apoio: serie diaria. Nao responde a nenhuma questao diretamente,
-- mas sustenta a validacao dos numeros e qualquer analise de sazonalidade.
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS refined.viagens_por_dia;

CREATE TABLE refined.viagens_por_dia AS
SELECT
    data_inicio                                    AS data,
    COUNT(*)                                       AS qtd_viagens,
    ROUND(AVG(distancia_milhas), 4)                AS media_distancia_milhas,
    ROUND(SUM(distancia_milhas), 2)                AS total_distancia_milhas,
    ROUND(AVG(duracao_minutos), 2)                 AS media_duracao_minutos,
    ROUND(SUM(valor_total), 2)                     AS receita_total
FROM trusted.viagens
GROUP BY data_inicio
ORDER BY data_inicio;

ALTER TABLE refined.viagens_por_dia ADD PRIMARY KEY (data);

COMMENT ON TABLE refined.viagens_por_dia IS
    'Serie diaria de volume, distancia e receita. Apoio a validacao.';

ANALYZE refined.resumo_volumetria;
ANALYZE refined.viagens_17_junho;
ANALYZE refined.viagem_mais_longa;
ANALYZE refined.estatisticas_distancia;
ANALYZE refined.viagens_por_dia;

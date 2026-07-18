-- Queries que respondem as quatro questoes do desafio.
--
-- Cada questao aparece em duas formas: a consulta direta sobre a camada
-- trusted, que e a resposta de fato, e a leitura da tabela materializada na
-- camada refined, equivalente e instantanea.
--
-- Uso: psql -U datarisk -d nyc_taxi -f 05_respostas.sql

\echo '==============================================================='
\echo 'QUESTAO 1 - Total de registros na tabela final'
\echo '==============================================================='

SELECT COUNT(*) AS total_registros
FROM trusted.viagens;

-- Visao completa, incluindo o que foi descartado na limpeza.
SELECT
    total_registros_raw,
    total_registros_trusted,
    total_descartado,
    percentual_descartado
FROM refined.resumo_volumetria;


\echo ''
\echo '==============================================================='
\echo 'QUESTAO 2 - Viagens iniciadas e finalizadas em 17 de junho'
\echo '==============================================================='

-- O enunciado admite mais de uma leitura, entao as tres sao calculadas.
SELECT
    COUNT(*) FILTER (WHERE data_inicio = DATE '2022-06-17') AS iniciadas,
    COUNT(*) FILTER (WHERE data_fim    = DATE '2022-06-17') AS finalizadas,
    COUNT(*) FILTER (
        WHERE data_inicio = DATE '2022-06-17'
          AND data_fim    = DATE '2022-06-17'
    ) AS iniciadas_e_finalizadas_no_mesmo_dia
FROM trusted.viagens
WHERE data_inicio = DATE '2022-06-17'
   OR data_fim    = DATE '2022-06-17';


\echo ''
\echo '==============================================================='
\echo 'QUESTAO 3 - Dia da viagem mais longa percorrida'
\echo '==============================================================='

-- Interpretacao principal: maior distancia percorrida.
SELECT
    data_inicio       AS dia,
    datahora_inicio,
    datahora_fim,
    distancia_milhas,
    distancia_km,
    duracao_minutos,
    valor_total
FROM trusted.viagens
ORDER BY distancia_milhas DESC
LIMIT 1;

-- Interpretacao alternativa: maior duracao.
SELECT
    data_inicio       AS dia,
    datahora_inicio,
    datahora_fim,
    duracao_minutos,
    distancia_milhas
FROM trusted.viagens
ORDER BY duracao_minutos DESC
LIMIT 1;


\echo ''
\echo '==============================================================='
\echo 'QUESTAO 4 - Estatisticas da distancia percorrida'
\echo '==============================================================='

SELECT
    COUNT(*)                                    AS qtd_viagens,
    ROUND(AVG(distancia_milhas), 4)             AS media,
    ROUND(STDDEV_SAMP(distancia_milhas), 4)     AS desvio_padrao,
    MIN(distancia_milhas)                       AS minimo,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (
        ORDER BY distancia_milhas)::NUMERIC, 4) AS q1_25pct,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
        ORDER BY distancia_milhas)::NUMERIC, 4) AS mediana_50pct,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (
        ORDER BY distancia_milhas)::NUMERIC, 4) AS q3_75pct,
    MAX(distancia_milhas)                       AS maximo
FROM trusted.viagens;

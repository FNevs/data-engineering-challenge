-- Camada trusted: dados limpos, tipados e validados.
--
-- O dataset publico do NYC TLC e notoriamente sujo. Os filtros abaixo tratam
-- problemas reais e observados nos arquivos de 2022; cada um esta documentado
-- com a sua justificativa, porque descartar registro sem explicar e o tipo de
-- decisao que invalida uma analise.
--
-- Premissa geral: e preferivel descartar um registro impossivel a deixar que
-- ele contamine medias e quartis. O volume descartado e medido e registrado em
-- log pela DAG, permitindo avaliar se algum filtro esta agressivo demais.

DROP TABLE IF EXISTS trusted.viagens;

CREATE TABLE trusted.viagens AS
SELECT
    -- Chave sintetica: o dataset de origem nao possui identificador de viagem.
    ROW_NUMBER() OVER (
        ORDER BY tpep_pickup_datetime, pu_location_id, do_location_id
    )                                              AS viagem_id,

    vendor_id::SMALLINT                            AS vendor_id,
    tpep_pickup_datetime                           AS datahora_inicio,
    tpep_dropoff_datetime                          AS datahora_fim,

    -- Colunas derivadas: evitam repetir CAST e EXTRACT nas consultas
    -- analiticas e permitem indexar diretamente a data.
    tpep_pickup_datetime::DATE                     AS data_inicio,
    tpep_dropoff_datetime::DATE                    AS data_fim,

    passenger_count::SMALLINT                      AS qtd_passageiros,
    trip_distance::NUMERIC(10, 2)                  AS distancia_milhas,

    -- O dataset e publicado em milhas. A conversao evita que quem consome a
    -- tabela precise lembrar da unidade e errar por isso.
    ROUND(trip_distance * 1.609344, 2)::NUMERIC(10, 2) AS distancia_km,

    ROUND(
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0,
        2
    )::NUMERIC(10, 2)                              AS duracao_minutos,

    rate_code_id::SMALLINT                         AS rate_code_id,
    store_and_fwd_flag                             AS flag_store_and_fwd,
    pu_location_id                                 AS local_embarque_id,
    do_location_id                                 AS local_desembarque_id,
    payment_type::SMALLINT                         AS tipo_pagamento,
    fare_amount::NUMERIC(10, 2)                    AS valor_corrida,
    extra::NUMERIC(10, 2)                          AS valor_extra,
    mta_tax::NUMERIC(10, 2)                        AS valor_imposto_mta,
    tip_amount::NUMERIC(10, 2)                     AS valor_gorjeta,
    tolls_amount::NUMERIC(10, 2)                   AS valor_pedagio,
    improvement_surcharge::NUMERIC(10, 2)          AS valor_sobretaxa,
    total_amount::NUMERIC(10, 2)                   AS valor_total,
    congestion_surcharge::NUMERIC(10, 2)           AS valor_congestionamento,
    airport_fee::NUMERIC(10, 2)                    AS valor_taxa_aeroporto,
    arquivo_origem
FROM raw.yellow_tripdata
WHERE
    -- (1) Datas obrigatorias. Sem elas a viagem nao e localizavel no tempo e
    --     nenhuma das questoes do desafio pode ser respondida.
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL

    -- (2) Coerencia temporal. Ha registros com desembarque anterior ao
    --     embarque, o que produziria duracao negativa.
    AND tpep_dropoff_datetime > tpep_pickup_datetime

    -- (3) Janela temporal do dataset. Os arquivos de 2022 contem uma minoria
    --     de registros com datas de 2001, 2008 e ate 2098, resultado de erro
    --     no equipamento de bordo. Manter esses registros distorceria a
    --     resposta sobre o dia da viagem mais longa.
    AND tpep_pickup_datetime >= '2022-01-01'::TIMESTAMP
    AND tpep_pickup_datetime <  '2023-01-01'::TIMESTAMP

    -- (4) Distancia positiva. Zero indica corrida cancelada ou erro de
    --     taximetro; negativo e fisicamente impossivel. Como as questoes 3 e 4
    --     sao justamente sobre distancia, esse filtro e o mais sensivel.
    AND trip_distance > 0

    -- (5) Teto de distancia. A maior corrida plausivel dentro da area de
    --     operacao dos taxis de NY nao ultrapassa algumas centenas de milhas.
    --     O dataset contem valores como 389678 milhas (equivalente a ida e
    --     volta a Lua), claramente defeito de hodometro. O corte em 1000
    --     milhas e generoso o bastante para preservar viagens interestaduais
    --     legitimas.
    AND trip_distance <= 1000

    -- (6) Duracao plausivel: entre 1 minuto e 24 horas. Descarta tanto
    --     registros de duracao nula quanto corridas em que o taximetro ficou
    --     ligado por dias.
    AND tpep_dropoff_datetime - tpep_pickup_datetime >= INTERVAL '1 minute'
    AND tpep_dropoff_datetime - tpep_pickup_datetime <= INTERVAL '24 hours'

    -- (7) Valor total nao negativo. Valores negativos correspondem a
    --     estornos, que nao representam corridas realizadas.
    AND total_amount >= 0;

-- Chave primaria sobre a chave sintetica.
ALTER TABLE trusted.viagens ADD PRIMARY KEY (viagem_id);

-- Indices que sustentam as consultas das questoes do desafio.
CREATE INDEX idx_trusted_data_inicio  ON trusted.viagens (data_inicio);
CREATE INDEX idx_trusted_data_fim     ON trusted.viagens (data_fim);
CREATE INDEX idx_trusted_distancia    ON trusted.viagens (distancia_milhas DESC);

-- ANALYZE atualiza as estatisticas do planner. Sem isso, o Postgres planeja as
-- consultas seguintes com base numa tabela que ele ainda considera vazia.
ANALYZE trusted.viagens;

COMMENT ON TABLE trusted.viagens IS
    'Viagens validadas de 2022. Ver 03_trusted.sql para os criterios de '
    'descarte aplicados.';

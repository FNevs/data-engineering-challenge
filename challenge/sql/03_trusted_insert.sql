-- Carga de um mes na camada trusted, aplicando limpeza, tipagem e validacao.
--
-- O parametro %(arquivo_origem)s identifica o arquivo de origem, o que torna a
-- carga idempotente e permite reprocessar um unico mes.
--
-- O dataset publico do NYC TLC e notoriamente sujo. Cada filtro abaixo trata um
-- problema real e observado nos arquivos de 2022, e vem com a sua justificativa
-- — descartar registro sem explicar e o tipo de decisao que invalida a analise.
--
-- Premissa geral: e preferivel descartar um registro impossivel a deixar que
-- ele contamine medias e quartis. O volume descartado e medido e registrado em
-- log a cada execucao, permitindo avaliar se algum filtro esta agressivo demais.

DELETE FROM trusted.viagens WHERE arquivo_origem = %(arquivo_origem)s;

INSERT INTO trusted.viagens (
    vendor_id, datahora_inicio, datahora_fim, data_inicio, data_fim,
    qtd_passageiros, distancia_milhas, distancia_km, duracao_minutos,
    rate_code_id, flag_store_and_fwd, local_embarque_id, local_desembarque_id,
    tipo_pagamento, valor_corrida, valor_extra, valor_imposto_mta,
    valor_gorjeta, valor_pedagio, valor_sobretaxa, valor_total,
    valor_congestionamento, valor_taxa_aeroporto, arquivo_origem
)
SELECT
    vendor_id::SMALLINT,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    tpep_pickup_datetime::DATE,
    tpep_dropoff_datetime::DATE,
    passenger_count::SMALLINT,
    trip_distance::NUMERIC(10, 2),
    ROUND(trip_distance * 1.609344, 2)::NUMERIC(10, 2),
    ROUND(
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0,
        2
    )::NUMERIC(10, 2),
    rate_code_id::SMALLINT,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type::SMALLINT,
    fare_amount::NUMERIC(10, 2),
    extra::NUMERIC(10, 2),
    mta_tax::NUMERIC(10, 2),
    tip_amount::NUMERIC(10, 2),
    tolls_amount::NUMERIC(10, 2),
    improvement_surcharge::NUMERIC(10, 2),
    total_amount::NUMERIC(10, 2),
    congestion_surcharge::NUMERIC(10, 2),
    airport_fee::NUMERIC(10, 2),
    arquivo_origem
FROM raw.yellow_tripdata
WHERE
    arquivo_origem = %(arquivo_origem)s

    -- (1) Datas obrigatorias. Sem elas a viagem nao e localizavel no tempo e
    --     nenhuma das questoes do desafio pode ser respondida.
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL

    -- (2) Coerencia temporal. Ha registros com desembarque anterior ao
    --     embarque, o que produziria duracao negativa.
    AND tpep_dropoff_datetime > tpep_pickup_datetime

    -- (3) Janela temporal do dataset. Os arquivos de 2022 contem uma minoria
    --     de registros com datas de 2001, 2008 e ate 2098, resultado de erro no
    --     equipamento de bordo. Manter esses registros distorceria a resposta
    --     sobre o dia da viagem mais longa.
    AND tpep_pickup_datetime >= '2022-01-01'::TIMESTAMP
    AND tpep_pickup_datetime <  '2023-01-01'::TIMESTAMP

    -- (4) Distancia positiva. Zero indica corrida cancelada ou erro de
    --     taximetro; negativo e fisicamente impossivel. Como as questoes 3 e 4
    --     sao sobre distancia, este filtro e o mais sensivel do conjunto.
    AND trip_distance > 0

    -- (5) Teto de distancia. A maior corrida plausivel na area de operacao dos
    --     taxis de NY nao passa de algumas centenas de milhas. O dataset traz
    --     valores como 389678 milhas — equivalente a ida e volta a Lua —,
    --     claramente defeito de hodometro. O corte em 1000 milhas e generoso o
    --     bastante para preservar viagens interestaduais legitimas.
    AND trip_distance <= 1000

    -- (6) Duracao plausivel: entre 1 minuto e 24 horas. Descarta tanto
    --     registros de duracao nula quanto corridas em que o taximetro ficou
    --     ligado por dias.
    AND tpep_dropoff_datetime - tpep_pickup_datetime >= INTERVAL '1 minute'
    AND tpep_dropoff_datetime - tpep_pickup_datetime <= INTERVAL '24 hours'

    -- (7) Valor total nao negativo. Valores negativos correspondem a estornos,
    --     que nao representam corridas realizadas.
    AND total_amount >= 0

    -- (8) Velocidade media plausivel: no maximo 100 mph.
    --
    --     Este filtro foi acrescentado depois de inspecionar o resultado da
    --     questao 3. Os filtros 4 a 6 sozinhos deixavam passar registros
    --     fisicamente impossiveis: a "viagem mais longa" era de 991,59 milhas
    --     percorridas em 26 minutos — 2.288 mph. O topo inteiro da distribuicao
    --     era assim, com velocidades implicitas de 1.200 a 38.000 mph.
    --
    --     O valor pago denunciava o defeito: US$ 25,67 por 991 milhas, quando
    --     uma corrida real dessa distancia custaria alguns milhares de dolares.
    --     Sao erros de hodometro ou GPS, nao corridas.
    --
    --     Filtrar por distancia ou duracao isoladamente nao resolve, porque
    --     cada valor e individualmente plausivel — o que e impossivel e a
    --     combinacao dos dois. Dai o criterio ser a razao entre eles.
    --
    --     100 mph e generoso de proposito: preserva viagens legitimas de longa
    --     distancia por rodovia, cuja media observada fica entre 45 e 65 mph.
    AND trip_distance / (
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600.0
    ) <= 100;

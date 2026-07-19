-- Tabela da camada raw.
--
-- Decisao: todas as colunas numericas do arquivo original sao mantidas como
-- NUMERIC e as datas como TIMESTAMP, mas sem NOT NULL e sem CHECK. A camada raw
-- nao rejeita registro algum: se o arquivo de origem traz uma distancia negativa
-- ou uma data de 2002, esse valor precisa chegar intacto ao banco. Validar aqui
-- destruiria a capacidade de auditar o que veio da fonte.
--
-- As colunas de controle (arquivo_origem, ingerido_em) dao rastreabilidade e
-- tornam a carga idempotente por arquivo.

DROP TABLE IF EXISTS raw.yellow_tripdata;

CREATE TABLE raw.yellow_tripdata (
    vendor_id              SMALLINT,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        NUMERIC,
    trip_distance          NUMERIC,
    rate_code_id           NUMERIC,
    store_and_fwd_flag     TEXT,
    pu_location_id         INTEGER,
    do_location_id         INTEGER,
    payment_type           NUMERIC,
    fare_amount            NUMERIC,
    extra                  NUMERIC,
    mta_tax                NUMERIC,
    tip_amount             NUMERIC,
    tolls_amount           NUMERIC,
    improvement_surcharge  NUMERIC,
    total_amount           NUMERIC,
    congestion_surcharge   NUMERIC,
    airport_fee            NUMERIC,
    -- Controle de proveniencia.
    arquivo_origem         TEXT        NOT NULL,
    ingerido_em            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE raw.yellow_tripdata IS
    'Ingestao fiel dos 12 arquivos yellow_tripdata de 2022. Sem transformacao.';
COMMENT ON COLUMN raw.yellow_tripdata.arquivo_origem IS
    'Nome do arquivo parquet de origem, usado para carga idempotente.';

-- Indice usado apenas para a limpeza idempotente (DELETE por arquivo antes de
-- recarregar). Nao ha indice analitico na raw: consultas analiticas devem rodar
-- sobre a trusted.
CREATE INDEX idx_raw_arquivo_origem
    ON raw.yellow_tripdata (arquivo_origem);

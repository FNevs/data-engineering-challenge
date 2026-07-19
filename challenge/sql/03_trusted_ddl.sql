-- Estrutura da camada trusted.
--
-- A tabela e criada vazia e populada em lotes mensais pela DAG (ver
-- 03_trusted_insert.sql). A versao anterior usava um unico CREATE TABLE AS
-- sobre os ~39,6M registros da raw: uma transacao de mais de 30 minutos que
-- segurava memoria do inicio ao fim e perdia todo o progresso a qualquer
-- interrupcao — o que de fato aconteceu duas vezes durante o desenvolvimento.
--
-- Carregar mes a mes divide o trabalho em 12 transacoes curtas. Cada uma
-- confirma seu proprio lote, entao uma falha custa um mes e nao o ano inteiro,
-- e o pico de memoria cai para um doze avos.

DROP TABLE IF EXISTS trusted.viagens;

CREATE TABLE trusted.viagens (
    -- IDENTITY em vez de ROW_NUMBER(): gera a chave de forma incremental,
    -- sem exigir que todas as linhas existam na mesma consulta. Isso e o que
    -- viabiliza a carga em lotes.
    viagem_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    vendor_id              SMALLINT,
    datahora_inicio        TIMESTAMP      NOT NULL,
    datahora_fim           TIMESTAMP      NOT NULL,

    -- Colunas derivadas: evitam repetir CAST e EXTRACT nas consultas
    -- analiticas e permitem indexar a data diretamente.
    data_inicio            DATE           NOT NULL,
    data_fim               DATE           NOT NULL,

    qtd_passageiros        SMALLINT,
    distancia_milhas       NUMERIC(10, 2) NOT NULL,

    -- O dataset e publicado em milhas. Manter a conversao materializada evita
    -- que quem consome a tabela precise lembrar da unidade e erre por isso.
    distancia_km           NUMERIC(10, 2) NOT NULL,
    duracao_minutos        NUMERIC(10, 2) NOT NULL,

    rate_code_id           SMALLINT,
    flag_store_and_fwd     TEXT,
    local_embarque_id      INTEGER,
    local_desembarque_id   INTEGER,
    tipo_pagamento         SMALLINT,
    valor_corrida          NUMERIC(10, 2),
    valor_extra            NUMERIC(10, 2),
    valor_imposto_mta      NUMERIC(10, 2),
    valor_gorjeta          NUMERIC(10, 2),
    valor_pedagio          NUMERIC(10, 2),
    valor_sobretaxa        NUMERIC(10, 2),
    valor_total            NUMERIC(10, 2),
    valor_congestionamento NUMERIC(10, 2),
    valor_taxa_aeroporto   NUMERIC(10, 2),
    arquivo_origem         TEXT           NOT NULL
);

COMMENT ON TABLE trusted.viagens IS
    'Viagens validadas de 2022. Ver 03_trusted_insert.sql para os criterios '
    'de descarte aplicados.';

-- Indice criado junto da tabela porque a carga mensal o consulta para garantir
-- idempotencia (DELETE por arquivo_origem antes de reinserir).
CREATE INDEX idx_trusted_arquivo_origem ON trusted.viagens (arquivo_origem);

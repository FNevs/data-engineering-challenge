-- Criacao das camadas do data warehouse.
--
-- A arquitetura em tres camadas separa responsabilidades e torna o pipeline
-- auditavel: quando um numero da camada refined parece errado, e possivel
-- descer ate a raw e comparar com o dado original sem reprocessar nada.
--
--   raw     : copia fiel do arquivo de origem, sem nenhuma transformacao.
--             Serve como fonte da verdade e permite reprocessar as camadas
--             seguintes sem baixar os arquivos novamente.
--   trusted : dado limpo, tipado e validado. Registros invalidos sao removidos
--             aqui, com o motivo de cada descarte registrado em log.
--   refined : agregacoes prontas para consumo analitico.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;

COMMENT ON SCHEMA raw IS
    'Ingestao fiel dos arquivos parquet do NYC TLC, sem transformacao.';
COMMENT ON SCHEMA trusted IS
    'Dados limpos, tipados e validados, prontos para analise.';
COMMENT ON SCHEMA refined IS
    'Agregacoes e metricas de negocio derivadas da camada trusted.';

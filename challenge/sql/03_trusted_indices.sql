-- Indices analiticos da camada trusted.
--
-- Criados apos a carga dos 12 meses, e nao junto da tabela, de proposito: manter
-- indices ativos durante a ingestao obriga o Postgres a atualiza-los a cada
-- linha inserida. Construi-los uma unica vez ao final e sensivelmente mais
-- rapido.

SET maintenance_work_mem = '512MB';

-- Sustentam as consultas das questoes 2 e 3.
CREATE INDEX IF NOT EXISTS idx_trusted_data_inicio
    ON trusted.viagens (data_inicio);
CREATE INDEX IF NOT EXISTS idx_trusted_data_fim
    ON trusted.viagens (data_fim);
CREATE INDEX IF NOT EXISTS idx_trusted_distancia
    ON trusted.viagens (distancia_milhas DESC);

-- ANALYZE atualiza as estatisticas do planner. Sem isso o Postgres planeja as
-- consultas seguintes acreditando que a tabela ainda esta vazia.
ANALYZE trusted.viagens;

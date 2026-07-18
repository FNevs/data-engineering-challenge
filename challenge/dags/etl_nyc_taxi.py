"""DAG de ETL dos dados de corridas de taxi amarelo de Nova York (2022).

Implementa a arquitetura de tres camadas exigida pelo desafio:

    extract  -> baixa os 12 arquivos .parquet.gz do repositorio da Datarisk
    raw      -> ingestao fiel, sem transformacao alguma
    trusted  -> limpeza, tipagem e remocao de registros invalidos
    refined  -> agregacoes que respondem as questoes do desafio

Cada mes e processado por uma task independente na etapa de carga raw, o que
permite reprocessar um unico mes sem refazer o ano inteiro.
"""

from __future__ import annotations

import gzip
import io
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final, List

import pandas as pd
import pyarrow.parquet as pq
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Constantes
# --------------------------------------------------------------------------- #

BASE_URL: Final[str] = (
    "https://raw.githubusercontent.com/datarisk-io/"
    "data-engineering-challenge/master/nyc-tlc-data"
)
ANO_REFERENCIA: Final[int] = 2022
MESES: Final[List[int]] = list(range(1, 13))
DIRETORIO_DADOS: Final[Path] = Path("/opt/airflow/data")
CONEXAO_WAREHOUSE: Final[str] = "warehouse"

# Tamanho do lote de leitura do parquet e de escrita via COPY. Os arquivos
# maiores passam de 3,5M linhas; ler tudo de uma vez pressionaria a memoria do
# container. 100k linhas por lote mantem o uso de RAM previsivel e ainda
# aproveita bem o throughput do COPY.
TAMANHO_CHUNK: Final[int] = 100_000

# Mapeia os nomes originais do parquet para snake_case, conforme o guia de
# estilo do projeto. O dataset do TLC usa PascalCase de forma inconsistente
# (VendorID, PULocationID, mas tpep_pickup_datetime em snake_case).
MAPA_COLUNAS: Final[dict[str, str]] = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}


# --------------------------------------------------------------------------- #
# Funcoes auxiliares
# --------------------------------------------------------------------------- #


def _nome_arquivo(mes: int) -> str:
    """Monta o nome do arquivo parquet de um mes de referencia.

    Parameters
    ----------
    mes : int
        Numero do mes, de 1 a 12.

    Returns
    -------
    str
        Nome do arquivo, no formato ``yellow_tripdata_2022-01.parquet``.

    Raises
    ------
    ValueError
        Se o mes estiver fora do intervalo de 1 a 12.
    """
    if mes not in range(1, 13):
        raise ValueError(f"Mes invalido: {mes}. Esperado um valor de 1 a 12.")

    return f"yellow_tripdata_{ANO_REFERENCIA}-{mes:02d}.parquet"


def carregar_parquet_em_raw(caminho_parquet: str) -> int:
    """Carrega um arquivo parquet na camada raw usando COPY FROM STDIN.

    A logica de carga vive fora do decorador ``@task`` de proposito: como
    funcao de modulo ela pode ser importada e testada isoladamente, sem exigir
    um contexto de execucao do Airflow.

    A carga e idempotente: registros previos do mesmo arquivo de origem sao
    removidos antes da insercao, de modo que reexecutar a task nao duplica
    dados. Tudo ocorre em uma unica transacao.

    Parameters
    ----------
    caminho_parquet : str
        Caminho absoluto do arquivo .parquet a ser carregado.

    Returns
    -------
    int
        Quantidade de linhas inseridas na camada raw.

    Raises
    ------
    FileNotFoundError
        Se o arquivo parquet informado nao existir.
    """
    arquivo = Path(caminho_parquet)
    if not arquivo.is_file():
        raise FileNotFoundError(f"Parquet nao encontrado: {caminho_parquet}")

    hook = PostgresHook(postgres_conn_id=CONEXAO_WAREHOUSE)
    conexao = hook.get_conn()
    total_linhas = 0

    try:
        with conexao.cursor() as cursor:
            # Garante idempotencia antes de qualquer escrita.
            cursor.execute(
                "DELETE FROM raw.yellow_tripdata WHERE arquivo_origem = %s",
                (arquivo.name,),
            )
            logger.info(
                "Removidos %d registros previos de %s",
                cursor.rowcount,
                arquivo.name,
            )

            # iter_batches le o parquet em lotes em vez de materializar o
            # arquivo inteiro: os meses maiores passam de 3,5M linhas e
            # carregar tudo de uma vez pressionaria a memoria do container.
            leitor = pq.ParquetFile(arquivo)
            colunas_destino = list(MAPA_COLUNAS.values()) + ["arquivo_origem"]
            comando_copy = (
                f"COPY raw.yellow_tripdata ({', '.join(colunas_destino)}) "
                "FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
            )

            for lote in leitor.iter_batches(batch_size=TAMANHO_CHUNK):
                df = lote.to_pandas()
                df = df.rename(columns=MAPA_COLUNAS)

                # airport_fee nao existe em todos os meses de 2022; preencher
                # aqui mantem o schema estavel entre os arquivos.
                for coluna in MAPA_COLUNAS.values():
                    if coluna not in df.columns:
                        df.loc[:, coluna] = pd.NA

                df.loc[:, "arquivo_origem"] = arquivo.name
                df = df.loc[:, colunas_destino]

                # COPY FROM STDIN e a via nativa de carga em massa do Postgres,
                # uma ordem de grandeza mais rapida que INSERTs. O buffer em
                # memoria evita escrever um CSV temporario em disco.
                buffer = io.StringIO()
                df.to_csv(
                    buffer,
                    index=False,
                    header=False,
                    sep="\t",
                    na_rep="\\N",
                )
                buffer.seek(0)

                cursor.copy_expert(comando_copy, buffer)
                total_linhas += len(df)

        conexao.commit()
    except Exception:
        conexao.rollback()
        logger.exception("Falha na carga de %s; transacao revertida.", arquivo.name)
        raise
    finally:
        conexao.close()

    logger.info("Concluida a carga de %s: %d linhas", arquivo.name, total_linhas)
    return total_linhas


def _executar_sql(caminho_sql: str) -> None:
    """Executa um arquivo SQL no banco de dados do warehouse.

    Parameters
    ----------
    caminho_sql : str
        Caminho absoluto do arquivo .sql a ser executado.

    Returns
    -------
    None
        A funcao nao retorna valor; o efeito e a execucao no banco.

    Raises
    ------
    FileNotFoundError
        Se o arquivo SQL informado nao existir.
    """
    arquivo = Path(caminho_sql)
    if not arquivo.is_file():
        raise FileNotFoundError(f"Arquivo SQL nao encontrado: {caminho_sql}")

    hook = PostgresHook(postgres_conn_id=CONEXAO_WAREHOUSE)
    logger.info("Executando script SQL: %s", arquivo.name)
    hook.run(arquivo.read_text(encoding="utf-8"), autocommit=True)


# --------------------------------------------------------------------------- #
# Definicao da DAG
# --------------------------------------------------------------------------- #


@dag(
    dag_id="etl_nyc_taxi",
    description=(
        "ETL das corridas de taxi amarelo de NY (2022) em camadas "
        "raw, trusted e refined."
    ),
    schedule=None,  # Carga historica pontual: disparo manual.
    start_date=datetime(2022, 1, 1),
    catchup=False,
    max_active_tasks=3,
    default_args={
        "owner": "filipe",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["datarisk", "nyc-tlc", "etl"],
)
def etl_nyc_taxi() -> None:
    """Orquestra o pipeline completo de ETL das corridas de taxi de NY.

    Returns
    -------
    None
        A funcao apenas declara a estrutura da DAG para o Airflow.
    """

    @task(task_id="criar_camada_raw")
    def criar_camada_raw() -> None:
        """Cria a tabela da camada raw, descartando a versao anterior.

        Returns
        -------
        None
            A funcao nao retorna valor; o efeito e a criacao da tabela.
        """
        _executar_sql("/opt/airflow/sql/02_raw_tables.sql")

    @task(task_id="extrair_arquivo")
    def extrair_arquivo(mes: int) -> str:
        """Baixa e descomprime o arquivo parquet de um mes.

        O download e ignorado caso o arquivo ja exista localmente, tornando a
        task barata em reprocessamentos.

        Parameters
        ----------
        mes : int
            Numero do mes a ser extraido, de 1 a 12.

        Returns
        -------
        str
            Caminho absoluto do arquivo .parquet descomprimido.

        Raises
        ------
        requests.HTTPError
            Se o download retornar um codigo de status de erro.
        """
        DIRETORIO_DADOS.mkdir(parents=True, exist_ok=True)

        nome_parquet = _nome_arquivo(mes)
        destino_parquet = DIRETORIO_DADOS / nome_parquet
        destino_gz = DIRETORIO_DADOS / f"{nome_parquet}.gz"

        if destino_parquet.is_file():
            logger.info("Arquivo ja existe, download ignorado: %s", nome_parquet)
            return str(destino_parquet)

        url = f"{BASE_URL}/{nome_parquet}.gz"
        logger.info("Baixando %s", url)

        # stream=True evita carregar os ~50 MB inteiros na memoria de uma vez.
        with requests.get(url, stream=True, timeout=300) as resposta:
            resposta.raise_for_status()
            with destino_gz.open("wb") as saida:
                for bloco in resposta.iter_content(chunk_size=1024 * 1024):
                    saida.write(bloco)

        # Apesar da extensao .gz, os arquivos publicados no repositorio do
        # desafio sao Parquet puro: a resposta HTTP nao traz Content-Encoding e
        # os primeiros bytes na rede ja sao a assinatura PAR1. Confiar na
        # extensao aqui quebraria a extracao com BadGzipFile.
        #
        # A deteccao e feita pelos magic bytes para que o pipeline funcione nos
        # dois casos, caso os arquivos sejam recomprimidos no futuro.
        with destino_gz.open("rb") as arquivo_baixado:
            assinatura = arquivo_baixado.read(4)

        if assinatura[:2] == b"\x1f\x8b":
            logger.info("Arquivo gzip detectado, descomprimindo %s", destino_gz.name)
            with gzip.open(destino_gz, "rb") as origem:
                with destino_parquet.open("wb") as saida:
                    shutil.copyfileobj(origem, saida)
            destino_gz.unlink()
        elif assinatura == b"PAR1":
            logger.info(
                "Arquivo ja e Parquet puro apesar da extensao .gz; "
                "renomeando %s",
                destino_gz.name,
            )
            destino_gz.rename(destino_parquet)
        else:
            destino_gz.unlink()
            raise ValueError(
                f"Formato nao reconhecido em {nome_parquet}.gz: "
                f"assinatura {assinatura!r}. Esperado gzip ou Parquet."
            )

        tamanho_mb = destino_parquet.stat().st_size / 1024 / 1024
        logger.info("Extraido %s (%.1f MB)", nome_parquet, tamanho_mb)

        return str(destino_parquet)

    @task(task_id="carregar_raw")
    def carregar_raw(caminho_parquet: str) -> int:
        """Carrega um arquivo parquet na camada raw, sem transformacao.

        Wrapper fino sobre :func:`carregar_parquet_em_raw`, que concentra a
        logica de carga em uma funcao de modulo testavel isoladamente.

        Parameters
        ----------
        caminho_parquet : str
            Caminho absoluto do arquivo .parquet a ser carregado.

        Returns
        -------
        int
            Quantidade de linhas inseridas na camada raw.
        """
        return carregar_parquet_em_raw(caminho_parquet)

    @task(task_id="construir_camada_trusted")
    def construir_camada_trusted(contagens: List[int]) -> int:
        """Constroi a camada trusted a partir da raw, aplicando as validacoes.

        Parameters
        ----------
        contagens : List[int]
            Quantidade de linhas carregada por cada task de carga raw. Serve
            para materializar a dependencia entre as etapas e para registrar o
            total ingerido em log.

        Returns
        -------
        int
            Quantidade de linhas resultante na camada trusted.
        """
        total_raw = sum(contagens)
        logger.info("Total ingerido na camada raw: %d linhas", total_raw)

        _executar_sql("/opt/airflow/sql/03_trusted.sql")

        hook = PostgresHook(postgres_conn_id=CONEXAO_WAREHOUSE)
        total_trusted = hook.get_first(
            "SELECT COUNT(*) FROM trusted.viagens"
        )[0]

        descartados = total_raw - total_trusted
        percentual = (descartados / total_raw * 100) if total_raw else 0.0
        logger.info(
            "Camada trusted: %d linhas mantidas, %d descartadas (%.2f%%).",
            total_trusted,
            descartados,
            percentual,
        )

        return total_trusted

    @task(task_id="construir_camada_refined")
    def construir_camada_refined(total_trusted: int) -> None:
        """Constroi as tabelas agregadas da camada refined.

        Parameters
        ----------
        total_trusted : int
            Quantidade de linhas da camada trusted, usada para encadear a
            dependencia entre as tasks e registrar o volume processado.

        Returns
        -------
        None
            A funcao nao retorna valor; o efeito e a criacao das tabelas.
        """
        logger.info(
            "Construindo camada refined sobre %d linhas da trusted.",
            total_trusted,
        )
        _executar_sql("/opt/airflow/sql/04_refined.sql")

    # ----------------------------------------------------------------------- #
    # Encadeamento das tasks
    # ----------------------------------------------------------------------- #

    tabela_raw = criar_camada_raw()

    # expand() cria uma task de extracao e uma de carga por mes, executadas com
    # o paralelismo definido em max_active_tasks.
    arquivos = extrair_arquivo.expand(mes=MESES)
    linhas_por_mes = carregar_raw.expand(caminho_parquet=arquivos)

    # A tabela precisa existir antes de qualquer carga.
    tabela_raw >> arquivos

    total_trusted = construir_camada_trusted(linhas_por_mes)
    construir_camada_refined(total_trusted)


etl_nyc_taxi()

"""Silver: Bronze -> eventos validados + estado do agregado (ADR-0002 / ADR-0005 / ADR-0006).

Funções puras (testáveis com Spark local):
  ``parse_events``   valida cada evento contra o schema do seu tipo no contrato; o resto vai p/ quarentena
  ``dedupe``         um registro por ``event_id`` (entrega at-least-once)
  ``fold_state``     reconstrói o estado do agregado a partir do histórico, em ordem de versão

Orquestração (Databricks): ``run`` lê a Bronze incrementalmente (availableNow) e, por micro-batch,
faz MERGE na tabela de eventos (insere só ids novos) e recalcula o estado dos agregados afetados
a partir da história completa, aplicando no MERGE só versões maiores. Resultado idempotente e
imune a eventos fora de ordem.
"""

from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from edp_etl.config import SilverSpec
from edp_etl.contract_schema import ContractSchemas

log = logging.getLogger(__name__)

EVENT_COLUMNS = [
    "event_id", "event_type", "event_source", "event_time", "aggregate_id", "aggregate_version", "data",
    "eh_partition", "eh_sequence_number", "_ingested_at", "_processed_at",
]


def _data_json() -> Column:
    return F.get_json_object(F.col("body"), "$.data")


def parse_events(bronze: DataFrame, schemas: ContractSchemas) -> tuple[DataFrame, DataFrame]:
    """Retorna (válidos, quarentena). Válido = tipo do contrato + campos obrigatórios presentes e bem tipados."""
    base = bronze.withColumn("data", _data_json()).withColumn("_processed_at", F.current_timestamp())

    valid_parts, invalid_parts = [], []
    for event_type, struct in schemas.by_type.items():
        typed = base.filter(F.col("event_type") == event_type).withColumn("_d", F.from_json("data", struct))
        missing = [
            F.when(F.col(f"_d.{name}").isNull(), F.lit(name)) for name in schemas.required[event_type]
        ]
        typed = typed.withColumn("_missing", F.array_compact(F.array(*missing)) if missing else F.array())
        ok = (F.size("_missing") == 0) & F.col("event_id").isNotNull() & F.col("aggregate_id").isNotNull()
        valid_parts.append(typed.filter(ok).drop("_d", "_missing"))
        invalid_parts.append(
            typed.filter(~ok).select(
                "*",
                F.concat(F.lit("campos obrigatórios ausentes ou com tipo inválido: "),
                         F.array_join("_missing", ", ")).alias("_motivo"),
            ).drop("_d", "_missing")
        )

    known = F.col("event_type").isin(schemas.event_types)
    invalid_parts.append(
        base.filter(~known | F.col("event_type").isNull()).withColumn(
            "_motivo", F.lit("tipo de evento fora da Published Language (contrato)")
        )
    )

    valid = reduce(DataFrame.unionByName, valid_parts).select(*EVENT_COLUMNS)
    quarantine = reduce(DataFrame.unionByName, invalid_parts).select(
        "event_id", "event_type", "aggregate_id", "body", "_motivo", "_source_file", "_processed_at"
    )
    return valid, quarantine


def dedupe(events: DataFrame) -> DataFrame:
    w = Window.partitionBy("event_id").orderBy(F.col("eh_sequence_number").asc_nulls_last())
    return events.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def fold_state(events: DataFrame, schemas: ContractSchemas, derived: dict[str, dict]) -> DataFrame:
    """Estado atual por agregado: para cada coluna, o último valor não nulo em ordem de versão (ECST)."""
    # Colunas derivadas (definidas para todos os tipos) prevalecem sobre campos homônimos do payload.
    fields = [
        f for f in schemas.state_fields() if f.name not in (schemas.key, "aggregate_version", *derived)
    ]

    per_type = []
    for event_type, struct in schemas.by_type.items():
        names = {f.name for f in struct.fields}
        d = F.from_json("data", struct)
        cols = [F.col("aggregate_id").alias(schemas.key), F.col("aggregate_version").alias("versao"),
                F.col("event_type").alias("ultimo_evento"), F.col("event_time").alias("ultimo_evento_em")]
        cols += [(d[f.name] if f.name in names else F.lit(None)).cast(f.dataType).alias(f.name) for f in fields]
        cols += [F.lit(values.get(event_type)).alias(col) for col, values in derived.items()]
        per_type.append(events.filter(F.col("event_type") == event_type).select(*cols))
    flat = reduce(DataFrame.unionByName, per_type)

    state_cols = [f.name for f in fields] + list(derived)
    history = Window.partitionBy(schemas.key).orderBy("versao").rowsBetween(Window.unboundedPreceding, 0)
    folded = flat.select(
        schemas.key, "versao", "ultimo_evento", "ultimo_evento_em",
        *[F.last(c, ignorenulls=True).over(history).alias(c) for c in state_cols],
    )
    latest = Window.partitionBy(schemas.key).orderBy(F.col("versao").desc())
    return (
        folded.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")
        .withColumn("_atualizado_em", F.current_timestamp())
    )


# ------------------------------------------------------------------ orquestração (Databricks)
def _table_exists(spark: SparkSession, name: str) -> bool:
    return spark.catalog.tableExists(name)


def _merge_events(spark: SparkSession, spec: SilverSpec, events: DataFrame) -> None:
    if not _table_exists(spark, spec.events_table):
        events.write.saveAsTable(spec.events_table)
        return
    events.createOrReplaceTempView("_edp_novos_eventos")
    spark.sql(f"""
        MERGE INTO {spec.events_table} t
        USING _edp_novos_eventos s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def _merge_state(spark: SparkSession, spec: SilverSpec, schemas: ContractSchemas, state: DataFrame) -> None:
    if not _table_exists(spark, spec.state_table):
        state.write.saveAsTable(spec.state_table)
        return
    state.createOrReplaceTempView("_edp_estado")
    spark.sql(f"""
        MERGE INTO {spec.state_table} t
        USING _edp_estado s
        ON t.{schemas.key} = s.{schemas.key}
        WHEN MATCHED AND s.versao > t.versao THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def process_batch(spark: SparkSession, spec: SilverSpec, schemas: ContractSchemas, bronze: DataFrame) -> None:
    valid, quarantine = parse_events(bronze, schemas)
    valid = dedupe(valid)

    if not quarantine.isEmpty():
        quarantine.write.mode("append").saveAsTable(spec.quarantine_table)
        log.warning("Eventos em quarentena: %d", quarantine.count())

    if valid.isEmpty():
        return
    _merge_events(spark, spec, valid)

    # Recalcula o estado dos agregados afetados a partir da história COMPLETA (ordem e replay seguros).
    affected = valid.select("aggregate_id").distinct()
    history = spark.table(spec.events_table).join(affected, "aggregate_id", "left_semi")
    _merge_state(spark, spec, schemas, fold_state(history, schemas, spec.derived))


def run(spark: SparkSession, spec: SilverSpec) -> None:
    schemas = ContractSchemas(spec.load_contract())
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {spec.catalog}.{spec.schema}._checkpoints")
    log.info("Silver %s / %s <- %s (contrato %s v%s)", spec.events_table, spec.state_table, spec.source,
             schemas.contract.doc.get("id"), schemas.contract.doc.get("version"))

    def _batch(df: DataFrame, batch_id: int) -> None:
        log.info("Micro-batch %s", batch_id)
        process_batch(df.sparkSession, spec, schemas, df)

    query = (
        spark.readStream.table(spec.source)
        .writeStream.foreachBatch(_batch)
        .option("checkpointLocation", spec.checkpoint_dir)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    from edp_etl.quality import run_quality

    run_quality(spark, spec, schemas)

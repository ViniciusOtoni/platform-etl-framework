import json
from pathlib import Path

import pytest
from edp_contracts.contract import Contract
from pyspark.sql import functions as F

from edp_etl.contract_schema import ContractSchemas
from edp_etl.quality import evaluate
from edp_etl.silver import EVENT_COLUMNS, dedupe, fold_state, parse_events

CONTRACT = Path(__file__).parent / "fixtures" / "pedidos.pedido.v1.odcs.yaml"
PID = "7b1f6c2e-5d1a-4c55-9a0e-2f6f3e9b8a11"
DERIVED = {"status": {
    "pedidos.pedido.criado.v1": "CRIADO",
    "pedidos.pedido.confirmado.v1": "CONFIRMADO",
    "pedidos.pedido.cancelado.v1": "CANCELADO",
}}

BRONZE_SCHEMA = (
    "body STRING, event_id STRING, event_type STRING, event_source STRING, event_time TIMESTAMP, subject STRING, "
    "aggregate_id STRING, aggregate_version INT, eh_partition INT, eh_offset STRING, eh_sequence_number BIGINT, "
    "eh_enqueued_time TIMESTAMP, _source_file STRING, _ingested_at TIMESTAMP"
)


@pytest.fixture(scope="module")
def schemas():
    return ContractSchemas(Contract.load(CONTRACT))


def criado(pid=PID, valor=268.9):
    return {"pedido_id": pid, "aggregate_version": 1, "cliente_id": "cli-1",
            "itens": [{"sku": "LIVRO", "quantidade": 1, "preco_unitario": valor}],
            "valor_total": valor, "moeda": "BRL", "criado_em": "2026-10-14T01:00:00.000+00:00"}


def confirmado(pid=PID):
    return {"pedido_id": pid, "aggregate_version": 2, "status": "CONFIRMADO",
            "confirmado_em": "2026-10-14T01:05:00.000+00:00"}


def cancelado(pid=PID):
    return {"pedido_id": pid, "aggregate_version": 3, "motivo": "CLIENTE_DESISTIU", "cancelado_por": "CLIENTE",
            "cancelado_em": "2026-10-14T01:10:00.000+00:00"}


def bronze(spark, events):
    """events: lista de (event_id, type, data, seq)."""
    rows = []
    for event_id, type_, data, seq in events:
        body = {"id": event_id, "type": type_, "aggregateid": data.get("pedido_id"),
                "aggregateversion": data.get("aggregate_version"), "data": data}
        version = data.get("aggregate_version")
        rows.append((json.dumps(body), event_id, type_, "/x", None, None, data.get("pedido_id"),
                     version if isinstance(version, int) else None, 0, str(seq), seq, None, "f.avro", None))
    return spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn("event_time", F.current_timestamp())


def test_contract_drives_spark_schema(schemas):
    assert schemas.key == "pedido_id"
    struct = schemas.by_type["pedidos.pedido.criado.v1"]
    assert struct["valor_total"].dataType.simpleString() == "double"
    assert struct["criado_em"].dataType.simpleString() == "timestamp"
    itens = "array<struct<sku:string,quantidade:bigint,preco_unitario:double>>"
    assert struct["itens"].dataType.simpleString() == itens
    assert {f.name for f in schemas.state_fields()} >= {"cliente_id", "motivo", "confirmado_em", "status"}
    assert [r["property"] for r in schemas.quality_rules()] == ["valor_total"]


def test_parse_events_validates_against_contract_and_quarantines(spark, schemas):
    sem_moeda = {k: v for k, v in criado("p-2").items() if k != "moeda"}
    df = bronze(spark, [
        ("e1", "pedidos.pedido.criado.v1", criado(), 1),
        ("e2", "pedidos.pedido.criado.v1", sem_moeda, 2),
        ("e3", "pedidos.pedido.estornado.v1", {"pedido_id": "p-3", "aggregate_version": 2}, 3),
        ("e4", "pedidos.pedido.confirmado.v1", dict(confirmado(), aggregate_version="dois"), 4),
    ])
    valid, quarantine = parse_events(df, schemas)

    assert valid.columns == EVENT_COLUMNS
    assert [r.event_id for r in valid.collect()] == ["e1"]
    motivos = {r.event_id: r._motivo for r in quarantine.collect()}
    assert set(motivos) == {"e2", "e3", "e4"}
    assert "moeda" in motivos["e2"]
    assert "Published Language" in motivos["e3"]
    assert "aggregate_version" in motivos["e4"]  # tipo inválido vira nulo no from_json => obrigatório ausente


def test_dedupe_keeps_one_record_per_event_id(spark, schemas):
    df = bronze(spark, [("e1", "pedidos.pedido.criado.v1", criado(), 5),
                        ("e1", "pedidos.pedido.criado.v1", criado(), 9)])
    valid, _ = parse_events(df, schemas)
    [row] = dedupe(valid).collect()
    assert row.eh_sequence_number == 5


def test_fold_state_is_order_independent_and_keeps_ecst_fields(spark, schemas):
    # Eventos chegam FORA de ordem (cancelado antes de criado): o estado final é o mesmo.
    df = bronze(spark, [
        ("e3", "pedidos.pedido.cancelado.v1", cancelado(), 1),
        ("e1", "pedidos.pedido.criado.v1", criado(), 2),
        ("e2", "pedidos.pedido.confirmado.v1", confirmado(), 3),
        ("e9", "pedidos.pedido.criado.v1", criado("p-9", 10.0), 4),
    ])
    valid, _ = parse_events(df, schemas)
    state = {r.pedido_id: r for r in fold_state(valid, schemas, DERIVED).collect()}

    p = state[PID]
    assert (p.versao, p.status, p.ultimo_evento) == (3, "CANCELADO", "pedidos.pedido.cancelado.v1")
    assert (p.cliente_id, p.valor_total, p.moeda) == ("cli-1", 268.9, "BRL")  # vieram do PedidoCriado
    assert p.motivo == "CLIENTE_DESISTIU" and p.confirmado_em is not None
    assert len(p.itens) == 1 and p.itens[0].sku == "LIVRO"
    assert (state["p-9"].versao, state["p-9"].status) == (1, "CRIADO")


def test_quality_comparators():
    assert evaluate({"mustBe": 0}, 0) == (True, "mustBe 0")
    assert evaluate({"mustBeLessThan": 5}, 7)[0] is False
    with pytest.raises(ValueError):
        evaluate({"threshold": 1}, 1)

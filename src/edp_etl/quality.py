"""Regras de qualidade declaradas no contrato ODCS (``quality: [{type: sql, query, mustBe...}]``).

``${object}`` na query é a tabela de estado da Silver. Resultados vão para ``<catalogo>.<schema>._dq_resultados``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from edp_etl.config import SilverSpec
from edp_etl.contract_schema import ContractSchemas

log = logging.getLogger(__name__)

_COMPARATORS = {
    "mustBe": lambda v, x: v == x,
    "mustNotBe": lambda v, x: v != x,
    "mustBeGreaterThan": lambda v, x: v > x,
    "mustBeGreaterOrEqualTo": lambda v, x: v >= x,
    "mustBeLessThan": lambda v, x: v < x,
    "mustBeLessOrEqualTo": lambda v, x: v <= x,
}


class QualidadeReprovada(Exception):
    pass


def evaluate(rule: dict, value) -> tuple[bool, str]:
    for key, check in _COMPARATORS.items():
        if key in rule:
            return check(value, rule[key]), f"{key} {rule[key]}"
    raise ValueError(f"regra sem comparador suportado: {sorted(rule)}")


def run_quality(spark: SparkSession, spec: SilverSpec, schemas: ContractSchemas) -> list[dict]:
    if not spark.catalog.tableExists(spec.state_table):
        return []
    results = []
    for rule in schemas.quality_rules():
        query = rule["query"].replace("${object}", spec.state_table)
        value = spark.sql(query).collect()[0][0]
        passed, expected = evaluate(rule, value)
        results.append({
            "tabela": spec.state_table,
            "propriedade": rule["property"],
            "regra": rule.get("description") or query,
            "query": query,
            "resultado": float(value) if value is not None else None,
            "esperado": expected,
            "aprovado": passed,
            "executado_em": datetime.now(UTC),
        })
        log.log(logging.INFO if passed else logging.WARNING, "DQ %s: %s (resultado=%s, %s)",
                "OK" if passed else "FALHA", rule.get("description"), value, expected)

    if results:
        spark.createDataFrame(results).write.mode("append").saveAsTable(spec.dq_table)
    failed = [r for r in results if not r["aprovado"]]
    if failed and spec.quality_on_failure == "fail":
        raise QualidadeReprovada(f"{len(failed)} regra(s) de qualidade reprovada(s): {[r['regra'] for r in failed]}")
    return results

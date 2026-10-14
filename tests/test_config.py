import pytest

from edp_etl.config import load

YAML = """
silver:
  source: ${catalog}.bronze.pedido_eventos_raw
  contract: ${contract}
  events_table: ${catalog}.silver.pedido_eventos
  state_table: ${catalog}.silver.pedido
  derived:
    status:
      pedidos.pedido.criado.v1: CRIADO
  quality:
    on_failure: fail
"""


def test_load_and_derived_names(tmp_path):
    path = tmp_path / "silver.yml"
    path.write_text(YAML, encoding="utf-8")
    contract = str(tmp_path / "c.yaml")
    spec = load(path, {"catalog": "pedidos_dev", "contract": contract})
    assert spec.quarantine_table == "pedidos_dev.silver.pedido_quarentena"
    assert spec.dq_table == "pedidos_dev.silver._dq_resultados"
    assert spec.checkpoint_dir == "/Volumes/pedidos_dev/silver/_checkpoints/pedido"
    assert spec.derived == {"status": {"pedidos.pedido.criado.v1": "CRIADO"}}
    assert spec.quality_on_failure == "fail"


def test_conventions_enforced(tmp_path):
    path = tmp_path / "silver.yml"
    path.write_text(YAML.replace("pedido_eventos", "eventos_pedido"), encoding="utf-8")
    with pytest.raises(ValueError, match="_eventos"):
        load(path, {"catalog": "pedidos_dev", "contract": "x"})

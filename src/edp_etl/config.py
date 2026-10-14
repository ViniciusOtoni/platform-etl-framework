"""Configuração declarada pelo domínio (``<dominio>-data/config/silver.yml``).

    silver:
      source: ${catalog}.bronze.pedido_eventos_raw
      contract: https://raw.githubusercontent.com/ViniciusOtoni/pedidos-service/<ref>/contracts/pedidos.pedido.v1.odcs.yaml
      events_table: ${catalog}.silver.pedido_eventos
      state_table: ${catalog}.silver.pedido
      derived:                    # colunas do estado derivadas do tipo do último evento
        status:
          pedidos.pedido.criado.v1: CRIADO
      quality:
        on_failure: warn          # warn | fail
"""

from __future__ import annotations

import re
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from edp_contracts.contract import Contract

_VAR = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
_TABLE = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+$")


def substitute(text: str, variables: dict[str, str]) -> str:
    def repl(m: re.Match) -> str:
        if m[1] not in variables:
            raise KeyError(f"variável ${{{m[1]}}} não informada (use --var {m[1]}=...)")
        return variables[m[1]]

    return _VAR.sub(repl, text)


@dataclass(frozen=True)
class SilverSpec:
    source: str
    contract: str
    events_table: str
    state_table: str
    derived: dict[str, dict[str, Any]] = field(default_factory=dict)
    quality_on_failure: str = "warn"

    @property
    def catalog(self) -> str:
        return self.state_table.split(".")[0]

    @property
    def schema(self) -> str:
        return self.state_table.split(".")[1]

    @property
    def quarantine_table(self) -> str:
        return f"{self.events_table.rsplit('_eventos', 1)[0]}_quarentena"

    @property
    def dq_table(self) -> str:
        return f"{self.catalog}.{self.schema}._dq_resultados"

    @property
    def checkpoint_dir(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/_checkpoints/{self.state_table.split('.')[2]}"

    def load_contract(self) -> Contract:
        """Contrato publicado pelo produtor (URL fixada numa revisão) ou arquivo local."""
        if self.contract.startswith(("https://", "http://")):
            with urllib.request.urlopen(self.contract, timeout=30) as resp:  # noqa: S310 - URL declarada pelo domínio
                return Contract.from_text(resp.read().decode("utf-8"))
        return Contract.load(self.contract)


def load(path: str | Path, variables: dict[str, str]) -> SilverSpec:
    doc = yaml.safe_load(substitute(Path(path).read_text(encoding="utf-8"), variables)) or {}
    s = doc.get("silver")
    if not s:
        raise ValueError(f"{path}: seção 'silver' ausente")
    spec = SilverSpec(
        source=s["source"],
        contract=s["contract"],
        events_table=s["events_table"],
        state_table=s["state_table"],
        derived=s.get("derived") or {},
        quality_on_failure=(s.get("quality") or {}).get("on_failure", "warn"),
    )
    for t in (spec.source, spec.events_table, spec.state_table):
        if not _TABLE.match(t):
            raise ValueError(f"tabela inválida {t!r}: use <catalogo>.<schema>.<tabela> em minúsculas")
    if not spec.events_table.endswith("_eventos"):
        raise ValueError("events_table deve terminar com '_eventos' (convenção da plataforma)")
    if spec.quality_on_failure not in ("warn", "fail"):
        raise ValueError("quality.on_failure deve ser warn ou fail")
    return spec

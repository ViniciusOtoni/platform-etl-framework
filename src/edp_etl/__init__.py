"""Framework Silver da plataforma (ADR-0005 / ADR-0006).

Bronze (CloudEvents brutos) -> Silver:
  - ``<agregado>_eventos``: história validada e deduplicada (por ``event_id``)
  - ``<agregado>``: estado atual do agregado, reconstruído do histórico (só versões maiores)
  - ``<agregado>_quarentena``: o que viola o contrato, com o motivo
  - ``_dq_resultados``: resultado das regras de qualidade declaradas no contrato
"""

__version__ = "0.1.0"

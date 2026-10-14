# platform-etl-framework (`edp-etl`)

Framework **Silver** da plataforma (ADR-0005 / ADR-0006). Transforma a Bronze (CloudEvents brutos) em dados confiáveis **dirigidos pelo contrato ODCS do produtor**, num job Databricks disparado por **table update**.

| Saída | Conteúdo |
|---|---|
| `<agregado>_eventos` | História validada e **deduplicada por `event_id`** (MERGE só insere ids novos) |
| `<agregado>` | **Estado atual** do agregado: último valor não nulo de cada campo, em ordem de `aggregate_version` |
| `<agregado>_quarentena` | Eventos fora da Published Language ou com campos obrigatórios ausentes/mal tipados, com `_motivo` |
| `_dq_resultados` | Resultado das regras `quality` do contrato |

## Como garante consistência

- **Schema = contrato.** Os tipos Spark são derivados do ODCS (`edp-contracts`), sem DDL escrita à mão.
- **At-least-once tolerado:** dedupe por `event_id` no batch e no MERGE.
- **Ordem tolerada:** a cada micro-batch, o estado dos agregados afetados é **recalculado a partir da história completa**, e o MERGE só aplica `versao` maior. Replays e eventos fora de ordem dão o mesmo resultado (há teste para isso).
- **ECST:** campos do `PedidoCriado` (cliente, itens, total) permanecem no estado depois de `PedidoCancelado`.
- **Colunas derivadas** por tipo de evento (ex.: `status`) prevalecem sobre campos homônimos do payload.

## Configuração (no repositório `<dominio>-data`)

```yaml
silver:
  source: ${catalog}.bronze.pedido_eventos_raw
  # Contrato publicado pelo produtor, fixado numa revisão (a Published Language é pública)
  contract: https://raw.githubusercontent.com/ViniciusOtoni/pedidos-service/${contract_ref}/contracts/pedidos.pedido.v1.odcs.yaml
  events_table: ${catalog}.silver.pedido_eventos
  state_table: ${catalog}.silver.pedido
  derived:
    status:
      pedidos.pedido.criado.v1: CRIADO
      pedidos.pedido.confirmado.v1: CONFIRMADO
      pedidos.pedido.cancelado.v1: CANCELADO
  quality:
    on_failure: warn     # warn | fail
```

## Desenvolvimento

```bash
pip install -e ".[dev]"     # inclui pyspark para testes locais (Java 17)
ruff check . && pytest
```

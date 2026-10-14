"""Entry point do job Silver (python_wheel_task, entry_point ``main``).

    edp-etl --config /Workspace/.../config/silver.yml --var catalog=pedidos_dev --var contract_ref=<sha>
"""

from __future__ import annotations

import argparse
import logging
import sys

from edp_etl.config import load


def _parse_vars(items: list[str]) -> dict[str, str]:
    out = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep:
            raise SystemExit(f"--var inválido {item!r}: use nome=valor")
        out[key] = value
    return out


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="edp-etl")
    parser.add_argument("--config", required=True)
    parser.add_argument("--var", action="append", default=[])
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    spec = load(args.config, _parse_vars(args.var))

    from pyspark.sql import SparkSession

    from edp_etl.silver import run

    run(SparkSession.builder.getOrCreate(), spec)
    return 0


if __name__ == "__main__":
    sys.exit(main())

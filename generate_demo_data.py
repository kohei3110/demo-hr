#!/usr/bin/env python3

"""Generate synthetic HR demo datasets (CSV) for Microsoft Fabric × Power BI demos.

Outputs are written as CSV files into the specified output directory (default: data/bronze).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from demo_hr.generator import GenerateConfig, generate_and_write


def _default_start_year(today: date) -> int:
    return today.year - 3


def _default_end_year(today: date) -> int:
    return today.year


@dataclass(frozen=True)
class CliArgs:
    seed: int | None
    outdir: Path
    employees: int
    start_year: int
    end_year: int
    newgrad_ratio: float


def parse_args(argv: list[str] | None = None) -> CliArgs:
    today = date.today()
    parser = argparse.ArgumentParser(description="Generate Fabric/Power BI HR demo CSV datasets")

    parser.add_argument("--seed", type=int, default=None, help="Deterministic seed (int)")
    parser.add_argument(
        "--outdir",
        type=Path,
        default=Path("data/bronze"),
        help="Output directory for CSVs (default: data/bronze)",
    )
    parser.add_argument(
        "--employees",
        type=int,
        default=2500,
        help="Number of employees to generate (default: 2500)",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=_default_start_year(today),
        help="Start year for time series data (default: current_year-3)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=_default_end_year(today),
        help="End year for time series data (default: current_year)",
    )
    parser.add_argument(
        "--newgrad-ratio",
        type=float,
        default=0.78,
        help="Fraction of employees who are new grads (default: 0.78)",
    )

    ns = parser.parse_args(argv)

    if ns.employees <= 0:
        raise SystemExit("--employees must be > 0")
    if not (0.0 <= ns.newgrad_ratio <= 1.0):
        raise SystemExit("--newgrad-ratio must be between 0 and 1")
    if ns.start_year > ns.end_year:
        raise SystemExit("--start-year must be <= --end-year")

    return CliArgs(
        seed=ns.seed,
        outdir=ns.outdir,
        employees=ns.employees,
        start_year=ns.start_year,
        end_year=ns.end_year,
        newgrad_ratio=ns.newgrad_ratio,
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = GenerateConfig(
        seed=args.seed,
        outdir=args.outdir,
        employees=args.employees,
        start_year=args.start_year,
        end_year=args.end_year,
        newgrad_ratio=args.newgrad_ratio,
    )

    generate_and_write(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import TechmartConfig, load_config
from .dimensions.dim_date import DIM_DATE_SPEC, build_dim_date
from .framework.writer import write_table


def generate(config: TechmartConfig, tables: list[str]) -> list[Path]:
    written: list[Path] = []
    for table in tables:
        if table == "dim_date":
            df = build_dim_date(config.start_date, config.end_date)
            written.append(write_table(df, DIM_DATE_SPEC, config.output_dir))
        else:
            raise ValueError(f"Unknown table: {table!r}")
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Techmart synthetic data.")
    parser.add_argument("--profile", default=None, help="Scale profile name.")
    parser.add_argument("--seed", type=int, default=42, help="Base RNG seed.")
    parser.add_argument("--output-dir", default="data", help="Output directory.")
    parser.add_argument(
        "--profiles-path",
        default="config/scale_profiles.yaml",
        help="Path to scale_profiles.yaml.",
    )
    parser.add_argument(
        "--tables",
        default="dim_date",
        help="Comma-separated table names to generate.",
    )
    args = parser.parse_args(argv)

    config = load_config(
        Path(args.profiles_path),
        args.profile,
        seed=args.seed,
        output_dir=Path(args.output_dir),
    )
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]
    paths = generate(config, tables)
    for path in paths:
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import TechmartConfig, load_config
from .framework.writer import write_table
from .registry import REGISTRY


def generate(config: TechmartConfig, tables: list[str]) -> list[Path]:
    written: list[Path] = []
    for table in tables:
        try:
            builder = REGISTRY[table]
        except KeyError:
            raise ValueError(f"Unknown table: {table!r}")
        df = builder.build(config)
        written.append(write_table(df, builder.spec, config.output_dir))
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

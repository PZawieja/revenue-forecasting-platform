"""
Realistic simulation mode: load config and write placeholder Parquet files for DuckDB/dbt consumption.
Generation logic (distributions, full entity data) to be implemented later.
"""

import argparse
from pathlib import Path

from forecasting.sim.src.io import write_parquet
from forecasting.sim.src.sim_config import load_config
from forecasting.sim.src.generators.gen_customers import generate_customers
from forecasting.sim.src.generators.gen_products import generate_products
from forecasting.sim.src.generators.gen_subscriptions import generate_subscriptions
from forecasting.sim.src.generators.gen_usage import generate_usage
from forecasting.sim.src.generators.gen_pipeline import generate_pipeline


def run(config_path: str | Path) -> None:
    """Load config and write placeholder Parquet files with correct names."""
    config = load_config(config_path)
    out = config.get("output", {})
    base_path = Path(out.get("base_path", "./warehouse/sim_data")).resolve()
    fmt = out.get("format", "parquet")

    if fmt != "parquet":
        raise ValueError(f"Unsupported output format: {fmt}")

    generators = [
        ("customers", generate_customers),
        ("products", generate_products),
        ("subscription_line_items", generate_subscriptions),
        ("pipeline_opportunities_snapshot", generate_pipeline),
        ("usage_monthly", generate_usage),
    ]
    for name, gen_fn in generators:
        df = gen_fn(config)
        path = base_path / f"{name}.parquet"
        write_parquet(df, path)
        print(f"Wrote {path} ({len(df)} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate simulation dataset (Parquet) for DuckDB/dbt.")
    parser.add_argument(
        "--config",
        default="forecasting/sim/config/sim_config.yml",
        help="Path to sim_config.yml (default: forecasting/sim/config/sim_config.yml)",
    )
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()

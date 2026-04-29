import argparse
import csv
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REQUIRED_COLUMNS = {"order_id", "customer_id", "amount", "region"}


class ETLConfigError(Exception):
    """Raised when required configuration (e.g., env vars) is missing/invalid."""


class ETLDataError(Exception):
    """Raised when input data validation fails."""


def configure_logging(log_level: Optional[str] = None) -> None:
    level_str = (log_level or os.getenv("LOG_LEVEL") or "INFO").upper()
    logging.basicConfig(
        level=level_str,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def get_base_dir() -> Path:
    # Resolve paths relative to this file so CI doesn't depend on CWD.
    return Path(__file__).resolve().parent


def get_default_paths() -> tuple[Path, Path]:
    base_dir = get_base_dir()
    input_json = base_dir / "Dataset" / "orders.json"
    output_csv = base_dir / "Dataset" / "revenue_by_region.csv"
    return input_json, output_csv


def extract_orders(json_path: Path) -> List[Dict[str, Any]]:
    try:
        logging.info("Extract: reading JSON from %s", json_path)
        if not json_path.exists():
            raise FileNotFoundError(f"Input JSON file not found: {json_path}")

        with json_path.open("r", encoding="utf-8") as infile:
            records: Iterable[Dict[str, Any]] = json.load(infile)

        records_list = list(records)
        if not isinstance(records_list, list):
            raise ValueError("Input JSON must be a list of order records")

        logging.info("Extract: loaded %d records", len(records_list))
        return records_list
    except Exception:
        logging.exception("Extract failed")
        raise


def validate_orders(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate data:
    - Check required columns
    - Remove rows with negative amounts
    """
    valid_records: List[Dict[str, Any]] = []
    removed_negative_count = 0

    for idx, record in enumerate(records, start=1):
        if not isinstance(record, dict):
            raise ETLDataError(f"Row {idx} is not an object/dict")

        missing = REQUIRED_COLUMNS - set(record.keys())
        if missing:
            raise ETLDataError(f"Row {idx} missing required columns: {sorted(missing)}")

        amount = record.get("amount")
        if amount is not None:
            if not isinstance(amount, (int, float)):
                raise ETLDataError(f"Row {idx} has non-numeric amount: {amount!r}")
            if amount < 0:
                removed_negative_count += 1
                continue

        valid_records.append(record)

    logging.info(
        "Validate: kept %d records (removed %d with negative amount)",
        len(valid_records),
        removed_negative_count,
    )
    return valid_records


def transform_orders(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform data:
    - Replace null amounts with 0
    - Aggregate total revenue by region
    """
    revenue_map: Dict[str, float] = {}

    for record in records:
        region = record["region"]
        amount = record["amount"]
        amount = 0 if amount is None else float(amount)
        revenue_map[region] = revenue_map.get(region, 0.0) + amount

    # Stable output for easier testing/CI diffs.
    rows = [{"region": r, "total_revenue": t} for r, t in sorted(revenue_map.items())]
    logging.info("Transform: aggregated into %d region rows", len(rows))
    return rows


def load_to_csv(rows: List[Dict[str, Any]], output_path: Path) -> None:
    """Load final data to CSV (mock Snowflake target)."""
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["region", "total_revenue"])
            writer.writeheader()
            writer.writerows(rows)
        logging.info("Load: wrote CSV to %s", output_path)
    except Exception:
        logging.exception("Load to CSV failed")
        raise


def _env_first_of(keys: List[str]) -> Optional[str]:
    for k in keys:
        v = os.getenv(k)
        if v is not None and v != "":
            return v
    return None


@dataclass(frozen=True)
class SnowflakeConfig:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    schema: str = "PUBLIC"


def get_snowflake_config_from_env() -> SnowflakeConfig:
    """
    Read Snowflake credentials from environment variables.

    Required env vars (prioritized with the raw names you requested, with
    fallback to SNOWFLAKE_* names):
    - SNOWFLAKE_USER / USER
    - SNOWFLAKE_PASSWORD / PASSWORD
    - SNOWFLAKE_ACCOUNT / ACCOUNT
    - SNOWFLAKE_WAREHOUSE / WAREHOUSE
    - SNOWFLAKE_DATABASE / DATABASE
    """
    user = _env_first_of(["USER", "SNOWFLAKE_USER"])
    password = _env_first_of(["PASSWORD", "SNOWFLAKE_PASSWORD"])
    account = _env_first_of(["ACCOUNT", "SNOWFLAKE_ACCOUNT"])
    warehouse = _env_first_of(["WAREHOUSE", "SNOWFLAKE_WAREHOUSE"])
    database = _env_first_of(["DATABASE", "SNOWFLAKE_DATABASE"])
    schema = _env_first_of(["SNOWFLAKE_SCHEMA"]) or "PUBLIC"

    missing: List[str] = []
    if not user:
        missing.append("SNOWFLAKE_USER/USER")
    if not password:
        missing.append("SNOWFLAKE_PASSWORD/PASSWORD")
    if not account:
        missing.append("SNOWFLAKE_ACCOUNT/ACCOUNT")
    if not warehouse:
        missing.append("SNOWFLAKE_WAREHOUSE/WAREHOUSE")
    if not database:
        missing.append("SNOWFLAKE_DATABASE/DATABASE")

    if missing:
        raise ETLConfigError(f"Missing Snowflake env vars: {', '.join(missing)}")

    return SnowflakeConfig(
        return SnowflakeConfig(
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
)
    )


def load_to_snowflake_mock(rows: List[Dict[str, Any]], config: SnowflakeConfig) -> None:
    """
    Mock Snowflake load.

    We do not connect to Snowflake in CI; we only validate config and log
    what would be loaded.
    """
    # Mask password in logs to avoid accidental credential leakage.
    masked_password = "*" * 8

    logging.info(
        "Snowflake mock load: would load %d rows into %s.%s.orders_summary "
        "(warehouse=%s, user=%s, password=%s)",
        len(rows),
        config.database,
        config.schema,
        config.warehouse,
        config.user,
        masked_password,
    )


def run_etl(
    input_json: Path,
    output_csv: Path,
    skip_snowflake_mock: bool = False,
) -> None:
    try:
        orders = extract_orders(input_json)
        valid_orders = validate_orders(orders)
        transformed = transform_orders(valid_orders)

        load_to_csv(transformed, output_csv)

        if not skip_snowflake_mock:
            # Config errors should not prevent CSV creation.
            try:
                snowflake_config = get_snowflake_config_from_env()
                load_to_snowflake_mock(transformed, snowflake_config)
            except ETLConfigError as exc:
                logging.warning("Snowflake mock load skipped: %s", exc)

        logging.info("ETL: completed successfully")
    except Exception:
        logging.exception("ETL run failed")
        raise


def main(argv: Optional[List[str]] = None) -> None:
    input_json_default, output_csv_default = get_default_paths()

    parser = argparse.ArgumentParser(description="ETL pipeline: orders -> revenue_by_region")
    parser.add_argument("--input-json", type=Path, default=input_json_default)
    parser.add_argument("--output-csv", type=Path, default=output_csv_default)
    parser.add_argument(
        "--skip-snowflake-mock",
        action="store_true",
        help="Skip the mock Snowflake load step (still writes the CSV).",
    )

    args = parser.parse_args(argv)
    run_etl(
        input_json=args.input_json,
        output_csv=args.output_csv,
        skip_snowflake_mock=args.skip_snowflake_mock,
    )


if __name__ == "__main__":
    configure_logging()
    main()
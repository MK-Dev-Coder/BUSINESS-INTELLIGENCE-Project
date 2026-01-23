from __future__ import annotations

import argparse

from bi.extract import extract_all
from bi.staging import load_staging
from bi.warehouse import build_warehouse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BI pipeline for FDA animal adverse events."
    )
    parser.add_argument(
        "--limit", type=int, default=2000, help="Number of FDA event records to fetch."
    )
    parser.add_argument(
        "--page-size", type=int, default=200, help="Page size for FDA API calls."
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=0.25,
        help="Delay between FDA API calls in seconds.",
    )
    parser.add_argument(
        "--extract-only", action="store_true", help="Run extraction only."
    )
    parser.add_argument(
        "--stage-only", action="store_true", help="Load staging DB only."
    )
    parser.add_argument(
        "--warehouse-only", action="store_true", help="Build warehouse only."
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.stage_only:
        load_staging()
        return
    if args.warehouse_only:
        build_warehouse()
        return

    extract_all(limit=args.limit, page_size=args.page_size, throttle_seconds=args.throttle)
    if args.extract_only:
        return

    load_staging()
    build_warehouse()


if __name__ == "__main__":
    main()

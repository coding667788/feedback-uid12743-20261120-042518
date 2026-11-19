"""Command line entry point: python -m solution --input cases.json --output result.json"""

import argparse
import sys

from .runner import run_cases


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="solution",
        description="Local permission revocation coordinator: replay cases.json "
        "against a fresh SQLite database and write result.json.",
    )
    parser.add_argument("--input", required=True, help="path to cases.json")
    parser.add_argument("--output", required=True, help="path to write result.json")
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite database path (default: <output>.sqlite3, recreated per run)",
    )
    args = parser.parse_args(argv)
    run_cases(args.input, args.output, args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())

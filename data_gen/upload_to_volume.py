"""Upload the generated files (data/raw/) to the landing Volume.

Usage (from the repo root, venv activated):
    python data_gen/upload_to_volume.py --schema claims_dev

Uses the Databricks SDK with the same authentication as the CLI
(`databricks auth login`): no token in the code.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from databricks.sdk import WorkspaceClient

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="claims_dev")
    args = parser.parse_args()

    landing = f"/Volumes/{args.catalog}/{args.schema}/landing"
    client = WorkspaceClient()
    for folder in ("claims", "policies"):
        for path in sorted((RAW_DIR / folder).glob("*")):
            target = f"{landing}/{folder}/{path.name}"
            with open(path, "rb") as f:
                client.files.upload(target, f, overwrite=True)
            print(f"uploaded {path.name} -> {target}")


if __name__ == "__main__":
    main()

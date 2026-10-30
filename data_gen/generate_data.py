"""Generate the sample source files for the claims pipeline.

Outputs (in data/raw/):
  claims/claims_2026_MM.csv   one CSV per month (Jan -> Jun 2026)
  policies/policies.jsonl     one JSON object per line
  expected_counts.json        the numbers you should find after each layer

Deliberate anomalies (detected by the Silver layer):
  - 2 negative amounts           -> quarantine (negative_amount)
  - 2 invalid dates              -> quarantine (invalid_date)
  - 1 unknown policy_id          -> quarantine (unknown_policy)
  - 3 claims re-sent next month  -> removed by deduplication (not quarantined)

Run locally (no Spark needed):  python data_gen/generate_data.py
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
MONTHS = [1, 2, 3, 4, 5, 6]
N_POLICIES = 300
CLAIMS_PER_MONTH = (100, 140)

CITIES = {"Rennes": 0.50, "Nantes": 0.35, "Brest": 0.15}
VEHICLE_TYPES = {"city_car": 0.45, "sedan": 0.30, "suv": 0.20, "van": 0.05}
# claim_type -> (weight, median amount in EUR)
CLAIM_TYPES = {
    "glass": (0.35, 400),
    "collision": (0.40, 2500),
    "theft": (0.08, 6000),
    "fire": (0.02, 8000),
    "natural_event": (0.15, 3000),
}
STATUSES = {"open": 0.25, "closed": 0.65, "rejected": 0.10}
CLAIM_COLUMNS = [
    "claim_id",
    "policy_id",
    "claim_date",
    "claim_amount",
    "claim_type",
    "status",
]


def weighted_choice(rng: random.Random, weights: dict) -> str:
    keys = list(weights)
    values = [w[0] if isinstance(w, tuple) else w for w in weights.values()]
    return rng.choices(keys, weights=values, k=1)[0]


def month_bounds(month: int) -> tuple[date, date]:
    start = date(2026, month, 1)
    next_month = date(2026, month + 1, 1) if month < 12 else date(2027, 1, 1)
    return start, next_month - timedelta(days=1)


def make_policies(rng: random.Random) -> list[dict]:
    policies = []
    for i in range(1, N_POLICIES + 1):
        start = date(2020, 1, 1) + timedelta(days=rng.randint(0, 6 * 365))
        policies.append(
            {
                "policy_id": f"POL-{i:05d}",
                "city": weighted_choice(rng, CITIES),
                "vehicle_type": weighted_choice(rng, VEHICLE_TYPES),
                "start_date": start.isoformat(),
                "annual_premium": round(rng.uniform(320, 1250), 2),
            }
        )
    return policies


def make_claims(rng: random.Random, policy_ids: list[str]) -> dict[int, list[dict]]:
    claims_by_month: dict[int, list[dict]] = {}
    counter = 0
    for month in MONTHS:
        start, end = month_bounds(month)
        rows = []
        for _ in range(rng.randint(*CLAIMS_PER_MONTH)):
            counter += 1
            claim_type = weighted_choice(rng, CLAIM_TYPES)
            median = CLAIM_TYPES[claim_type][1]
            amount = round(rng.lognormvariate(0, 0.6) * median, 2)
            day = start + timedelta(days=rng.randint(0, (end - start).days))
            rows.append(
                {
                    "claim_id": f"CLM-{counter:06d}",
                    "policy_id": rng.choice(policy_ids),
                    "claim_date": day.isoformat(),
                    "claim_amount": f"{amount:.2f}",
                    "claim_type": claim_type,
                    "status": weighted_choice(rng, STATUSES),
                }
            )
        claims_by_month[month] = rows
    return claims_by_month


def inject_anomalies(
    rng: random.Random, claims_by_month: dict[int, list[dict]]
) -> dict:
    """Inject documented anomalies and return the expected quarantine size."""
    # 1) Negative amounts (Feb, Apr) - fixed rows so anomalies never overlap
    for month in (2, 4):
        row = claims_by_month[month][10]
        row["claim_amount"] = f"-{row['claim_amount']}"
    # 2) Invalid dates: impossible day + wrong format
    claims_by_month[2][5]["claim_date"] = "2026-02-30"
    claims_by_month[3][7]["claim_date"] = "31/03/2026"
    # 3) Unknown policy (referential integrity)
    claims_by_month[5][3]["policy_id"] = "POL-99999"
    # 4) Duplicates: 3 valid claims from month m re-sent in month m+1's file
    clean = lambda r: (
        not r["claim_amount"].startswith("-")
        and r["claim_date"][4] == "-"
        and r["claim_date"] != "2026-02-30"
        and r["policy_id"] != "POL-99999"
    )
    for month in (1, 3, 5):
        candidate = next(r for r in claims_by_month[month] if clean(r))
        claims_by_month[month + 1].append(dict(candidate))
    return {"quarantine": 5, "duplicates": 3}


def write_outputs(
    policies: list[dict], claims_by_month: dict[int, list[dict]], anomalies: dict
) -> None:
    claims_dir = OUTPUT_DIR / "claims"
    policies_dir = OUTPUT_DIR / "policies"
    claims_dir.mkdir(parents=True, exist_ok=True)
    policies_dir.mkdir(parents=True, exist_ok=True)

    for month, rows in claims_by_month.items():
        with open(
            claims_dir / f"claims_2026_{month:02d}.csv",
            "w",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.DictWriter(f, fieldnames=CLAIM_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    with open(policies_dir / "policies.jsonl", "w", encoding="utf-8") as f:
        f.writelines(json.dumps(p) + "\n" for p in policies)

    raw_rows = sum(len(r) for r in claims_by_month.values())
    unique_rows = raw_rows - anomalies["duplicates"]
    expected = {
        "bronze_claims_rows": raw_rows,
        "duplicates_removed": anomalies["duplicates"],
        "quarantine_claims_rows": anomalies["quarantine"],
        "silver_claims_rows": unique_rows - anomalies["quarantine"],
        "silver_policies_rows": len(policies),
    }
    (OUTPUT_DIR / "expected_counts.json").write_text(json.dumps(expected, indent=2))
    print(json.dumps(expected, indent=2))


def sanity_check(policies: list[dict], claims_by_month: dict[int, list[dict]]) -> None:
    """Fail fast if the generated data is not what the course expects."""
    known = {p["policy_id"] for p in policies}
    all_rows = [r for rows in claims_by_month.values() for r in rows]
    orphans = [r for r in all_rows if r["policy_id"] not in known]
    negatives = [r for r in all_rows if r["claim_amount"].startswith("-")]
    ids = [r["claim_id"] for r in all_rows]
    assert len(orphans) == 1, orphans
    assert len(negatives) == 2, negatives
    assert len(ids) - len(set(ids)) == 3, "expected exactly 3 duplicated claim_ids"


def main() -> None:
    rng = random.Random(SEED)
    policies = make_policies(rng)
    claims_by_month = make_claims(rng, [p["policy_id"] for p in policies])
    anomalies = inject_anomalies(rng, claims_by_month)
    sanity_check(policies, claims_by_month)
    write_outputs(policies, claims_by_month, anomalies)
    print(f"Files written to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

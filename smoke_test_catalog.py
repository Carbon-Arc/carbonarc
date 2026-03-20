"""
Smoke test for CatalogAPIClient against a local power-api instance.

Usage:
    python smoke_test_catalog.py

Prerequisites:
    1. Local power-api running:   cd ../power-api && uvicorn app.main:app --reload --port 8000
    2. ENVIRONMENT=dev in power-api/.env.local  (bypasses auth — any token works)
    3. This package installed in editable mode:  pip install -e .
"""

import json
import sys

from carbonarc import CarbonArcClient

HOST = "http://localhost:8000"
TOKEN = "dev"  # any string works in dev mode


def print_result(label: str, result: dict) -> None:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(json.dumps(result, indent=2, default=str))


def main():
    client = CarbonArcClient(token=TOKEN, host=HOST)
    catalog = client.catalog

    # ── 1. List all assets ────────────────────────────────────────
    result = catalog.list_assets()
    print_result("list_assets()", result)
    assets = result.get("assets", [])
    total = result.get("total", 0)
    print(f"\n  → {total} asset(s) returned")

    if not assets:
        print("\nNo assets found — seed the database or check the server.")
        sys.exit(0)

    # Grab a couple of IDs for further tests
    first_id = assets[0]["id"]
    gated_asset = next((a for a in assets if a.get("visibility") != "public"), None)

    # ── 2. Filters ────────────────────────────────────────────────
    print_result("list_assets(sort='newest')", catalog.list_assets(sort="newest"))
    print_result("list_assets(sort='popularity')", catalog.list_assets(sort="popularity"))

    # ── 3. Get single asset ───────────────────────────────────────
    print_result(f"get_asset({first_id})", catalog.get_asset(first_id))

    # ── 4. Trending ───────────────────────────────────────────────
    print_result("get_trending(limit=5)", catalog.get_trending(limit=5))

    # ── 5. Vote status ────────────────────────────────────────────
    print_result(f"get_vote_status({first_id})", catalog.get_vote_status(first_id))

    # ── 6. Toggle vote (adds vote) ────────────────────────────────
    print_result(f"toggle_vote({first_id})  [add]", catalog.toggle_vote(first_id))

    # ── 7. Toggle vote again (removes vote) ──────────────────────
    print_result(f"toggle_vote({first_id})  [remove]", catalog.toggle_vote(first_id))

    # ── 8. Request access (gated/locked asset only) ───────────────
    if gated_asset:
        gated_id = gated_asset["id"]
        print_result(
            f"request_access({gated_id}, type='access')",
            catalog.request_access(gated_id, request_type="access"),
        )
    else:
        print("\n  [skip] request_access — no gated/locked assets found")

    print("\n\nAll checks complete.")


if __name__ == "__main__":
    main()

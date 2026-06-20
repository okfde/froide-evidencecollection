#!/usr/bin/env python3
"""Bake edited image descriptions from the ledger into a copy of import.json.

The ledger only stores human *edits*; posts reviewed and approved as-is keep the
generated text already in import.json. So this overrides `image_alt_text.alt_text`
only where the ledger carries an `alt_text_edited`, leaving `text_bezug_zum_bild`
and everything else untouched. The result is written to a new file (e.g.
import.curated.json) that the DB importer reads, so edited descriptions reach the
database and survive every prepare_import.py re-run — the curation lives in the
ledger, not in the regenerable import.json.

Run after a review round, before importing to the DB:

    python scripts/apply_image_reviews.py scripts/data/import.json
"""

import argparse
import json
from pathlib import Path

from image_reviews_common import (
    ensure_parent,
    iter_review_posts,
    load_json,
    load_ledger,
)


def apply_reviews(import_json, ledger_path, out):
    data = load_json(import_json)
    ledger = load_ledger(ledger_path)

    applied = 0
    for key, _platform, item in iter_review_posts(data):
        edited = (ledger.get(key) or {}).get("alt_text_edited")
        if not edited:  # absent / approved as-is → keep the generated text
            continue
        # A hand-described image may have no image_alt_text yet; create the dict,
        # otherwise keep its other keys (e.g. text_bezug_zum_bild).
        alt = item.get("image_alt_text")
        if not isinstance(alt, dict):
            alt = item["image_alt_text"] = {}
        alt["alt_text"] = edited
        applied += 1

    ensure_parent(out).write_text(json.dumps(data, ensure_ascii=False, indent=2))
    print(f"Applied {applied} edited description(s) to {out}.")
    return applied


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("import_json", type=Path, help="Path to import.json.")
    parser.add_argument(
        "--ledger",
        default="scripts/data/image_reviews.json",
        help="Review ledger to apply (default: scripts/data/image_reviews.json).",
    )
    parser.add_argument(
        "--out",
        default="scripts/data/import.curated.json",
        help="Curated JSON output (default: scripts/data/import.curated.json).",
    )
    args = parser.parse_args()
    apply_reviews(args.import_json, args.ledger, args.out)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import csv
import json
from pathlib import Path


def load(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def iter_rows(items: dict):
    for _person_id, person in items.items():
        social_media = person.get("social_media") or {}
        for platform, platform_data in social_media.items():
            for post in platform_data:
                yield {
                    "URL": post["url"],
                    "Datum": post["created_at"][:10],
                    "Vorname": person.get("Vorname"),
                    "Nachname": person.get("Nachname"),
                    "Bundesland": post.get("Bundesland"),
                    "Funktion": post.get("Funktion"),
                    "Gewichtung": post.get("Gewichtung"),
                    "Plattform": platform,
                    "Username": post.get("account", {}).get("username"),
                    "Titel": post.get("title", ""),
                    "Text": post.get("text", ""),
                    "Transkription": post.get("transcription", ""),
                    "Kategorien": ", ".join(set(post.get("categories", []))),
                }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert entries of a JSON file into a CSV file."
    )
    parser.add_argument("input", type=Path, help="Path to the input JSON file.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("import.csv"),
        help="Output CSV path (default: import.csv).",
    )
    args = parser.parse_args()

    items = load(args.input)

    fieldnames = [
        "URL",
        "Datum",
        "Vorname",
        "Nachname",
        "Bundesland",
        "Funktion",
        "Gewichtung",
        "Plattform",
        "Username",
        "Titel",
        "Text",
        "Transkription",
        "Kategorien",
    ]
    with args.output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in iter_rows(items):
            writer.writerow(row)

    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()

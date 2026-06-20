#!/usr/bin/env python3
"""Diff two JSON files, ignoring the order of elements.

Object key order never matters in JSON, and here array order does not either: two
arrays are equal when they hold the same elements regardless of position. The diff
reports three kinds of change, each with a JSON-pointer-ish path:

    - path : value      only in the first file  (removed)
    + path : value      only in the second file (added)
    ~ path : a -> b     present in both but changed

Array elements are matched as a multiset, so a reordered list shows no diff while
a genuinely added/removed item shows up once. When two array elements look like
the same entity that changed (they share an id or several field values), they are
paired and the diff descends into them, reporting only the fields that differ
rather than the whole object twice. Exit status is 1 when the files differ and 0
when they are equal, so the script is usable in a pipeline:

    python scripts/json_diff.py a.json b.json
"""

import argparse
import json
import sys
from pathlib import Path


def load(path: Path):
    with path.open() as f:
        return json.load(f)


def canonical(value) -> str:
    """A stable string key for a value, with objects/arrays order-normalised."""
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def fmt(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def diff(a, b, path="", out=None, top=True):
    """Collect diff lines comparing ``a`` (first file) against ``b`` (second).

    ``top`` marks the outermost collection: a whole entry added or removed there
    is reported by key alone, since dumping its entire payload is just noise. One
    level down (``top=False``) the value is shown so changes stay legible.
    """
    if out is None:
        out = []

    if isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(set(a) | set(b)):
            child = f"{path}/{key}"
            if key not in a:
                out.append(
                    f"+ {child}{_label(b[key])}"
                    if top
                    else f"+ {child} : {fmt(b[key])}"
                )
            elif key not in b:
                out.append(
                    f"- {child}{_label(a[key])}"
                    if top
                    else f"- {child} : {fmt(a[key])}"
                )
            else:
                diff(a[key], b[key], child, out, top=False)
    elif isinstance(a, list) and isinstance(b, list):
        _diff_list(a, b, path, out)
    elif canonical(a) != canonical(b):
        out.append(f"~ {path} : {fmt(a)} -> {fmt(b)}")

    return out


def _diff_list(a, b, path, out):
    """Compare two lists ignoring element order, descending into changed items."""
    # 1. Cancel out exact matches as a multiset, keeping only the leftovers.
    leftover_b = list(b)
    rem_a = []
    for x in a:
        for i, y in enumerate(leftover_b):
            if canonical(x) == canonical(y):
                del leftover_b[i]
                break
        else:
            rem_a.append(x)
    rem_b = leftover_b

    # 2. Pair leftovers that look like the same entity (best similarity first),
    #    so a one-field change descends instead of printing the whole object.
    candidates = sorted(
        (
            (_similarity(x, y), i, j)
            for i, x in enumerate(rem_a)
            for j, y in enumerate(rem_b)
            if _similarity(x, y) > 0
        ),
        reverse=True,
    )
    used_a, used_b, pairs = set(), set(), []
    for _score, i, j in candidates:
        if i in used_a or j in used_b:
            continue
        used_a.add(i)
        used_b.add(j)
        pairs.append((i, j))

    for i, j in sorted(pairs):
        diff(rem_a[i], rem_b[j], f"{path}[{_hint(rem_a[i], rem_b[j])}]", out, False)
    for i, x in enumerate(rem_a):
        if i not in used_a:
            out.append(f"- {_elem_label(path, x)}")
    for j, y in enumerate(rem_b):
        if j not in used_b:
            out.append(f"+ {_elem_label(path, y)}")


def _similarity(a, b) -> float:
    """How likely two array elements are the same entity that changed.

    Zero means "treat as unrelated" (a clean add/remove); a positive score pairs
    them so the diff can descend and show only the differing fields.
    """
    if isinstance(a, dict) and isinstance(b, dict):
        shared = set(a) & set(b)
        equal = sum(1 for k in shared if canonical(a[k]) == canonical(b[k]))
        # Need at least one field that agrees; otherwise treat the two as
        # distinct entities (a clean add/remove) rather than one big mutation.
        if equal == 0:
            return 0
        return equal + (0.5 if a.keys() == b.keys() else 0)
    if isinstance(a, list) and isinstance(b, list):
        return 1
    return 0


# Keys tried, in order, to identify an array element in a path label.
ID_KEYS = ("id", "pk", "uuid", "url", "slug", "name", "key")


def _identity(x) -> str:
    """An ``id=value`` label for a dict element, or "" if it has no id-like key."""
    if isinstance(x, dict):
        for key in ID_KEYS:
            if x.get(key) is not None:
                return f"{key}={fmt(x[key])}"
    return ""


def _hint(a, b) -> str:
    """A stable identifier for a paired element, for a meaningful path label."""
    if isinstance(a, dict) and isinstance(b, dict):
        for key in ID_KEYS:
            if a.get(key) is not None and a.get(key) == b.get(key):
                return f"{key}={fmt(a[key])}"
    return ""


def _elem_label(path, x) -> str:
    """Label an added/removed element by its identity alone when it has one.

    A dict with an id-like key shows just ``path[id=…]`` (plus its ``label`` for
    a human-readable hint); anything else (a dict with no identifier, or a
    scalar) shows its value, which is its own identity.
    """
    ident = _identity(x)
    if ident:
        return f"{path}[{ident}]{_label(x)}"
    return f"{path}[] : {fmt(x)}"


def _label(value) -> str:
    """A `` : <label>`` suffix for an entry shown by key, or "" if it has none.

    The hash/id that keys an entry is opaque; its ``label`` field (e.g. a name)
    says which one it is, so include it on otherwise key-only add/remove lines.
    """
    if isinstance(value, dict) and value.get("label") is not None:
        return f" : {fmt(value['label'])}"
    return ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diff two JSON files, ignoring the order of elements."
    )
    parser.add_argument("first", type=Path, help="Path to the first JSON file.")
    parser.add_argument("second", type=Path, help="Path to the second JSON file.")
    args = parser.parse_args()

    lines = diff(load(args.first), load(args.second))
    if not lines:
        print("Files are equal (ignoring element order).")
        sys.exit(0)

    print("\n".join(lines))
    sys.exit(1)


if __name__ == "__main__":
    main()

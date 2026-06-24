import re

from django import template
from django.template.defaultfilters import linebreaksbr
from django.utils.html import escape
from django.utils.safestring import mark_safe
from django.utils.text import normalize_newlines

register = template.Library()


# Okabe-Ito-derived 8-colour palette: distinguishable for the common forms of
# colour-vision deficiency and >= 3:1 contrast against a near-white card
# background. The colour is always rendered *alongside* the entity's name,
# so it's redundant signal — never the sole identifier.
PALETTE = (
    "#0072B2",  # blue
    "#009E73",  # bluish green
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
    "#E69F00",  # orange
    "#56B4E9",  # sky blue
    "#6A4C93",  # purple
    "#777777",  # neutral grey (also: "unknown")
)


@register.filter
def plain_text(value):
    """Render plain text safely with line breaks.

    - Converts literal '\\n' sequences to real newlines
    - Escapes HTML and converts newlines to <br> tags (via linebreaksbr)
    """
    value = (value or "").replace("\\n", "\n")
    return linebreaksbr(value)


@register.simple_tag
def highlight_keywords(value, keywords):
    """Render text like `plain_text`, with keyword surface forms marked.

    `keywords` is an iterable of `Keyword`; the keys of each one's
    `surface_forms` are the literal variants that appeared in the corpus, so we
    match those (case-insensitive, whole-word) rather than the normalised lemma.
    The text is HTML-escaped first, matches are wrapped in `<mark>`, then
    newlines become `<br>` — same safe output as `plain_text`, plus highlights.
    With no keywords (or none matching) it degrades to plain escaped text.
    """
    text = normalize_newlines((value or "").replace("\\n", "\n"))
    escaped = escape(text)

    # Collect every surface form across the evidence's keywords. Longest first
    # so a multi-word form ("soziale medien") matches before its constituent
    # words would. Escape each form the same way as the haystack so the pattern
    # stays aligned with the escaped text (e.g. an apostrophe → `&#x27;`).
    forms = set()
    for kw in keywords or []:
        forms.update(getattr(kw, "surface_forms", None) or {})
    forms = sorted((f for f in forms if f and f.strip()), key=len, reverse=True)

    if forms:
        # (?<!\w)…(?!\w) is a Unicode-aware word boundary (umlauts/ß count as
        # word chars), so we don't highlight a form sitting inside a longer word.
        pattern = re.compile(
            r"(?<!\w)(?:" + "|".join(re.escape(escape(f)) for f in forms) + r")(?!\w)",
            re.IGNORECASE,
        )
        escaped = pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", escaped)

    return mark_safe(escaped.replace("\n", "<br>"))


@register.filter
def break_after_commas(value):
    """Render a ``", "``-joined string with a line break after each comma.

    Used for the topic-cloud table's multi-value cells (each originator with its
    Verband, the chapters) so the comma-separated entries stack one per line.
    Splits on the exact ``", "`` join separator (so a comma inside a single
    entry is left alone), HTML-escapes each piece, and keeps the comma before
    the ``<br>``.
    """
    parts = [escape(p) for p in (value or "").split(", ")]
    return mark_safe(",<br>".join(parts))


@register.filter
def palette_color(obj):
    """Map any model instance (or pk) to a stable palette colour.

    Used to colour category stripes and chips. The colour is decorative — the
    category name is always rendered as text alongside, so colour is never
    the sole identifier.
    """
    if obj is None:
        return PALETTE[-1]
    pk = getattr(obj, "pk", obj)
    try:
        idx = int(pk) % len(PALETTE)
    except (TypeError, ValueError):
        return PALETTE[-1]
    return PALETTE[idx]


@register.filter
def compact_number(value):
    """Format an int compactly: 1500 → '1.5K', 2_400_000 → '2.4M'."""
    if value is None or value == "":
        return ""
    try:
        n = int(value)
    except (TypeError, ValueError):
        return value
    if abs(n) < 1000:
        return str(n)
    for divisor, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if abs(n) >= divisor:
            text = f"{n / divisor:.1f}".rstrip("0").rstrip(".")
            return f"{text}{suffix}"
    return str(n)

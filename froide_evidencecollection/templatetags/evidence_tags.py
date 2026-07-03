from django import template
from django.template.defaultfilters import linebreaksbr
from django.utils.html import escape
from django.utils.safestring import mark_safe

register = template.Library()


@register.filter
def plain_text(value):
    """Render plain text safely with line breaks.

    - Converts literal '\\n' sequences to real newlines
    - Escapes HTML and converts newlines to <br> tags (via linebreaksbr)
    """
    value = (value or "").replace("\\n", "\n")
    return linebreaksbr(value)


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
def defang_url(value):
    """Render a URL as plain, non-clickable text with ``https`` → ``httpx``.

    Used for the original-post URL on the evidence detail page: we want the
    address shown verbatim but not turned into a live link.
    """
    return (value or "").replace("https", "httpx")


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

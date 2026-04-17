from django import template
from django.template.defaultfilters import linebreaksbr

register = template.Library()


@register.filter
def plain_text(value):
    """Render plain text safely with line breaks.

    - Converts literal '\\n' sequences to real newlines
    - Escapes HTML and converts newlines to <br> tags (via linebreaksbr)
    """
    value = value.replace("\\n", "\n")
    return linebreaksbr(value)

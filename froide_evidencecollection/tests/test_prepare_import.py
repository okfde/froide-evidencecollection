"""Tests for the alt-text merge in scripts/prepare_import.py.

prepare_import builds import.json from scraped JSON and can fold in an external
LLM alt-text batch, joined to posts by the image filename stem. The scripts
directory is added to the path so the module imports like it does when run as
`python scripts/prepare_import.py`.
"""

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))

import prepare_import  # noqa: E402


def test_load_alt_texts_keeps_only_ok_nonempty(tmp_path):
    path = tmp_path / "alt.json"
    path.write_text(
        json.dumps(
            [
                {
                    "id": "111",
                    "status": "OK",
                    "alt_text": "  a caption  ",
                    "error": None,
                },
                {"id": "222", "status": "ERROR", "alt_text": "", "error": "boom"},
                {"id": "333", "status": "OK", "alt_text": "   ", "error": None},
            ]
        )
    )
    assert prepare_import.load_alt_texts(path) == {"111": "a caption"}


def test_attach_alt_text_matches_on_image_stem():
    alt_map = {"111": "external caption"}
    post = {
        "image_file": "./images/111.jpg",
        "image_alt_text": {"text_bezug_zum_bild": "JA"},
    }
    prepare_import.attach_alt_text(post, alt_map)
    # External text wins; sibling keys are preserved.
    assert post["image_alt_text"] == {
        "text_bezug_zum_bild": "JA",
        "alt_text": "external caption",
    }


def test_attach_alt_text_creates_dict_when_absent():
    post = {"image_file": "./images/111.jpg"}
    prepare_import.attach_alt_text(post, {"111": "caption"})
    assert post["image_alt_text"] == {"alt_text": "caption"}


def test_attach_alt_text_noop_without_image_or_match():
    no_image = {"text": "hi"}
    prepare_import.attach_alt_text(no_image, {"111": "caption"})
    assert "image_alt_text" not in no_image

    no_match = {"image_file": "./images/999.jpg"}
    prepare_import.attach_alt_text(no_match, {"111": "caption"})
    assert "image_alt_text" not in no_match


def test_clean_social_media_threads_alt_map():
    post = {
        "url_corrected": "u",
        "image_file": "./images/111.jpg",
        "timestamp": 1700000000,
        "comments_count": 0,
        "reshare_count": 0,
        "author": {"url": "https://www.facebook.com/x", "id": "1", "name": "X"},
        "attached_post": None,
    }
    cleaned = prepare_import.clean_social_media(
        {"facebook": [post]}, {"111": "caption"}
    )
    assert cleaned["facebook"][0]["image_alt_text"]["alt_text"] == "caption"

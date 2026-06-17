# screenshot_tool

Local batch processing for social-media post screenshots. No cloud, no API keys.

**blur** — anonymise a folder of *screenshots*: blur the whole image while
keeping detected text sharp/readable. Text regions are detected with EasyOCR.

## Setup

```bash
python3 -m venv .venv-ocr
.venv-ocr/bin/pip install -r requirements.txt
```

The first run downloads the EasyOCR detection/recognition models (cached in
`~/.EasyOCR/`).

## Usage

```bash
# blur screenshots, keep text sharp -> output folder
.venv-ocr/bin/python screenshot_tool.py --in screenshots/ --out blurred/
```

### Options

- `--lang de,en` — OCR languages for text detection (default German+English).
- `--method gaussian|pixelate` — blur style. `pixelate` is an irreversible
  mosaic; prefer it when the point is to truly anonymise faces.
- `--strength 3.0` — higher = blurrier.
- `--pad 6` — pixels each text box is expanded so glyph edges stay crisp.
- `--inpaint-pad`- pixels each text box is expanded to remove from background to prevent halos
- `--feather` - amount to feather the text mask, see --strength
- `--gpu` — use GPU if a CUDA build of torch is installed.

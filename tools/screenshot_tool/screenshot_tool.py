#!/usr/bin/env python3
"""
Anonymise social-media screenshots, fully locally (EasyOCR + OpenCV).

  blur  Anonymise every *screenshot* in a folder: blur the whole image but
        keep the detected text regions sharp/readable, then write the result
        to an output folder.

Examples
--------
  python screenshot_tool.py blur --in screenshots/ --out blurred/

Notes
-----
* Text regions are detected with EasyOCR so they can be kept sharp; the OCR
  language defaults to German+English (--lang de,en).
* Blur method defaults to a strong Gaussian; use --method pixelate for an
  irreversible mosaic (recommended when the goal is true anonymisation of
  faces, since heavy pixelation cannot be "unblurred").
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import cv2
import numpy as np

IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def find_images(folder: Path) -> list[Path]:
    if not folder.is_dir():
        sys.exit(f"error: not a directory: {folder}")
    files = sorted(p for p in folder.iterdir() if p.suffix.lower() in IMAGE_EXTS)
    if not files:
        print(f"warning: no images found in {folder}", file=sys.stderr)
    return files


_READER = None


def get_reader(langs: list[str], use_gpu: bool):
    """Build the EasyOCR reader once and reuse it (model load is slow)."""
    global _READER
    if _READER is None:
        import easyocr  # imported lazily so --help is instant

        print(
            f"loading OCR model (languages={langs}, gpu={use_gpu}) ...", file=sys.stderr
        )
        _READER = easyocr.Reader(langs, gpu=use_gpu)
    return _READER


# --------------------------------------------------------------------------- #
# Blur (anonymise screenshots, keep text sharp)
# --------------------------------------------------------------------------- #
def blur_whole(img: np.ndarray, method: str, strength: float) -> np.ndarray:
    h, w = img.shape[:2]
    if method == "pixelate":
        # downscale to a tiny grid, then nearest-neighbour back up -> mosaic
        scale = max(0.01, min(0.2, strength / 100.0))
        small = cv2.resize(
            img,
            (max(1, int(w * scale)), max(1, int(h * scale))),
            interpolation=cv2.INTER_LINEAR,
        )
        return cv2.resize(small, (w, h), interpolation=cv2.INTER_NEAREST)
    # gaussian: sigma scales with image size * strength
    sigma = max(1.0, (min(h, w) / 100.0) * strength)
    return cv2.GaussianBlur(img, (0, 0), sigmaX=sigma, sigmaY=sigma)


def text_mask(img: np.ndarray, boxes: list, pad: int) -> np.ndarray:
    """White (255) wherever text should stay sharp; black elsewhere."""
    mask = np.zeros(img.shape[:2], dtype=np.uint8)
    for bbox in boxes:
        pts = np.array(bbox, dtype=np.int32)
        cv2.fillPoly(mask, [pts], 255)
    if pad > 0:
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (pad, pad))
        mask = cv2.dilate(mask, kernel)
    return mask


def run_blur(
    in_dir: Path,
    out_dir: Path,
    langs: list[str],
    use_gpu: bool,
    method: str,
    strength: float,
    pad: int,
) -> None:
    reader = get_reader(langs, use_gpu)
    images = find_images(in_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for i, path in enumerate(images, 1):
        print(f"[blur {i}/{len(images)}] {path.name}", file=sys.stderr)
        img = cv2.imread(str(path))
        if img is None:
            print(f"  failed to read {path.name}", file=sys.stderr)
            continue
        try:
            detections = reader.readtext(str(path))
        except Exception as exc:
            print(f"  OCR failed ({exc}); blurring entire image", file=sys.stderr)
            detections = []

        boxes = [bbox for bbox, _text, _conf in detections]
        mask = text_mask(img, boxes, pad)
        blurred = blur_whole(img, method, strength)
        # keep sharp text where mask is white, blurred elsewhere
        out = np.where(mask[:, :, None] == 255, img, blurred)

        out_path = out_dir / path.name
        cv2.imwrite(str(out_path), out)

    print(f"wrote blurred images -> {out_dir}", file=sys.stderr)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_langs(value: str) -> list[str]:
    return [s.strip() for s in value.split(",") if s.strip()]


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--in", dest="in_dir", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument(
        "--lang",
        type=parse_langs,
        default=parse_langs("de,en"),
        help="OCR languages, comma-separated (default: de,en)",
    )
    p.add_argument(
        "--gpu",
        action="store_true",
        help="use GPU if a CUDA build of torch is installed",
    )
    p.add_argument(
        "--method",
        choices=["gaussian", "pixelate"],
        default="gaussian",
        help="blur style (default: gaussian; pixelate = irreversible mosaic)",
    )
    p.add_argument(
        "--strength",
        type=float,
        default=3.0,
        help="blur strength; higher = blurrier (default: 3.0)",
    )
    p.add_argument(
        "--pad",
        type=int,
        default=6,
        help="pixels to expand each text box so edges stay crisp (default: 6)",
    )

    args = p.parse_args()

    run_blur(
        args.in_dir,
        args.out,
        args.lang,
        args.gpu,
        args.method,
        args.strength,
        args.pad,
    )


if __name__ == "__main__":
    main()

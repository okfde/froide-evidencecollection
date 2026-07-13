"""Compute each evidence's 2D embedding coordinates for the cloud view.

Each evidence's input text is `Evidence.topic_text` — the source text assembled
highest-signal-first, since the embedding model truncates to a token window. The
text is embedded with a sentence-transformers model, and the dot *positions*
come from a 2D UMAP pass over those embeddings.

This command set `topic_fit_at` alongside `topic_x` / `topic_y`; the cloud view
reads the cached numbers.

Requires: sentence-transformers, umap-learn, numpy. Install the bundled extra
with `uv pip install -e '../froide-evidencecollection[topics]'`.
"""

import hashlib
import os
import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from froide_evidencecollection.models import Evidence

# Default embeddings cache: a .npz under the repo-root .cache/ dir (gitignored).
# Resolved from this file's location so it's stable regardless of cwd:
# commands → management → froide_evidencecollection → repo root.
DEFAULT_EMBEDDINGS_CACHE = str(
    Path(__file__).resolve().parents[3] / ".cache" / "topic_embeddings.npz"
)


class Command(BaseCommand):
    help = "Compute each evidence's 2D embedding coordinates for the cloud view."

    def add_arguments(self, parser):
        parser.add_argument(
            "--min-text-len",
            type=int,
            default=30,
            help=(
                "Skip evidence whose assembled text is shorter than this "
                "(default: 30)."
            ),
        )
        parser.add_argument(
            "--embedding-model",
            default="paraphrase-multilingual-MiniLM-L12-v2",
            help=(
                "sentence-transformers model name. Default is multilingual "
                "to handle German content."
            ),
        )
        parser.add_argument(
            "--max-seq-length",
            type=int,
            default=256,
            help=(
                "Token window the embedding model reads; text beyond it is "
                "truncated. The default model ships with 128, but most evidence "
                "exceeds that — 256 captures the typical document; the "
                "architectural max is ~512 (default: 256)."
            ),
        )
        parser.add_argument(
            "--embeddings-cache",
            default=None,
            help=(
                "Path to a .npz embeddings cache. Document embeddings are "
                "loaded from here and only new/changed evidence (keyed by text "
                "hash) is re-encoded, then the cache is rewritten — so an "
                "unchanged corpus skips the encode pass entirely. The cache is "
                "tagged with the embedding model + max-seq-length and ignored "
                "if either differs. Defaults to a gitignored file under the "
                "repo's .cache/; pass a path to override, or "
                "--no-embeddings-cache to disable."
            ),
        )
        parser.add_argument(
            "--no-embeddings-cache",
            action="store_true",
            help="Disable the embeddings cache: always re-encode every document.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Only fit the first N pieces of evidence (for quick experiments).",
        )
        parser.add_argument(
            "--reset",
            action="store_true",
            help=(
                "Also clear coords + fit marker on evidence that doesn't end "
                "up in the fitted set (e.g. too short, dropped by --limit)."
            ),
        )

    def handle(self, *args, **options):
        try:
            from sentence_transformers import SentenceTransformer
            from umap import UMAP
        except ImportError as exc:
            raise CommandError(
                "Missing dependency: %s. Install with "
                "`uv pip install sentence-transformers umap-learn`." % exc.name
            ) from exc

        # Resolve the effective embeddings cache: explicit path wins, then the
        # gitignored project default, unless disabled. Normalise to None or a
        # concrete .npz path (np.savez appends .npz, so match it for loading).
        if options["no_embeddings_cache"]:
            options["embeddings_cache"] = None
        elif not options["embeddings_cache"]:
            options["embeddings_cache"] = DEFAULT_EMBEDDINGS_CACHE
        if options["embeddings_cache"] and not options["embeddings_cache"].endswith(
            ".npz"
        ):
            options["embeddings_cache"] += ".npz"

        min_len = options["min_text_len"]
        # select_related/prefetch_related cover everything Evidence.topic_text
        # touches, so iterating doesn't fan out to per-row queries.
        qs = (
            Evidence.objects.all()
            .select_related(
                "social_media_post__account",
                "social_media_post__redistributes__account",
            )
            .prefetch_related("social_media_post__redaction_rules", "mentions")
            .order_by("pk")
        )
        if options["limit"]:
            qs = qs[: options["limit"]]

        evidences: list[Evidence] = []
        docs: list[str] = []
        for ev in qs.iterator(chunk_size=500):
            text = ev.topic_text
            if len(text) < min_len:
                continue
            evidences.append(ev)
            docs.append(text)

        # UMAP needs a handful of neighbours to build the 2D projection.
        if len(docs) < 5:
            raise CommandError(
                f"Need at least 5 usable pieces of evidence; got {len(docs)}. "
                "Lower --min-text-len or import more evidence."
            )

        self.stdout.write(
            f"Embedding {len(docs)} pieces of evidence with "
            f"{options['embedding_model']} (max_seq_length="
            f"{options['max_seq_length']})…"
        )
        embedder = SentenceTransformer(options["embedding_model"])
        embedder.max_seq_length = options["max_seq_length"]

        # Load the doc embedding cache (vectors keyed by text hash). Encode only
        # what's missing; the file is rewritten once it's filled, below.
        doc_cache = self._load_embedding_cache(
            options["embeddings_cache"],
            options["embedding_model"],
            options["max_seq_length"],
        )
        hashes = [hashlib.sha1(d.encode("utf-8")).hexdigest() for d in docs]
        t0 = time.perf_counter()
        embeddings, n_doc_enc = self._encode_with_cache(
            hashes, docs, doc_cache, embedder
        )
        self.stdout.write(
            f"  docs: {len(docs) - n_doc_enc} cached, {n_doc_enc} encoded "
            f"in {time.perf_counter() - t0:.1f}s."
        )

        # The cache is now fully populated for this run — persist once.
        self._save_embedding_cache(
            options["embeddings_cache"],
            options["embedding_model"],
            options["max_seq_length"],
            doc_cache,
        )

        self.stdout.write("Reducing embeddings to 2D for the cloud view…")
        # n_neighbors must stay below the sample count; clamp for small corpora.
        n_neighbors = min(15, len(docs) - 1)
        coords = UMAP(
            n_components=2,
            n_neighbors=n_neighbors,
            min_dist=0.1,
            metric="cosine",
            random_state=42,
        ).fit_transform(embeddings)

        now = timezone.now()

        with transaction.atomic():
            for ev, (x, y) in zip(evidences, coords, strict=True):
                ev.topic_x = float(x)
                ev.topic_y = float(y)
                ev.topic_fit_at = now

            Evidence.objects.bulk_update(
                evidences, ["topic_x", "topic_y", "topic_fit_at"], batch_size=500
            )

            if options["reset"]:
                fitted_ids = {e.pk for e in evidences}
                Evidence.objects.exclude(pk__in=fitted_ids).update(
                    topic_x=None,
                    topic_y=None,
                    topic_fit_at=None,
                )

        self.stdout.write(
            self.style.SUCCESS(f"Done. Fitted {len(evidences)} pieces of evidence.")
        )

    def _load_embedding_cache(self, cache_path, model_name, max_seq_length):
        """Load the doc embedding cache from ``cache_path``.

        Returns a ``text-hash -> vector`` dict. Empty when caching is off, the
        file is absent/unreadable, or it was written with a different embedding
        model / max-seq-length — a vector means nothing under a different
        encoder, so a mismatch silently recomputes everything.
        """
        import numpy as np

        doc_cache: dict[str, "np.ndarray"] = {}
        if not cache_path or not os.path.exists(cache_path):
            return doc_cache
        try:
            data = np.load(cache_path, allow_pickle=False)
            if (
                str(data["model"]) != model_name
                or int(data["max_seq_length"]) != max_seq_length
            ):
                self.stdout.write(
                    "  embeddings cache ignored (model or max-seq-length "
                    "differs); recomputing."
                )
                return {}
            doc_cache = dict(
                zip(data["doc_keys"].tolist(), data["doc_vectors"], strict=False)
            )
        except Exception as exc:  # corrupt / old-format / unreadable → recompute
            self.stdout.write(f"  embeddings cache unreadable ({exc}); recomputing.")
            return {}
        return doc_cache

    def _encode_with_cache(self, keys, texts, cache, embedder):
        """Embed ``texts`` (aligned to ``keys``), encoding only the entries
        whose key isn't already in ``cache`` and adding the new vectors to it.

        Returns ``(embeddings, n_encoded)`` where ``embeddings`` is a 2D array
        in ``texts`` order. ``cache`` is mutated in place so the caller can
        persist the grown set afterwards.
        """
        import numpy as np

        miss = [i for i, k in enumerate(keys) if k not in cache]
        if miss:
            new_vecs = np.asarray(
                embedder.encode([texts[i] for i in miss], show_progress_bar=True)
            )
            for j, i in enumerate(miss):
                cache[keys[i]] = new_vecs[j]
        return np.asarray([cache[k] for k in keys]), len(miss)

    def _save_embedding_cache(self, cache_path, model_name, max_seq_length, doc_cache):
        """Persist the doc cache to a single ``.npz``, tagged with the encoder.

        Writes the full known set (prior entries plus this run's), so later
        ``--limit`` or incremental runs keep previously computed vectors.
        """
        import numpy as np

        if not cache_path:
            return
        os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
        np.savez(
            cache_path,
            model=np.array(model_name),
            max_seq_length=np.array(max_seq_length),
            doc_keys=np.array(list(doc_cache.keys())),
            doc_vectors=np.asarray(list(doc_cache.values()))
            if doc_cache
            else np.empty((0, 0)),
        )

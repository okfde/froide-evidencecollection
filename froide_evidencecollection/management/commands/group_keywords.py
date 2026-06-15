"""Cluster the fitted `Keyword`s into broad cross-corpus themes and (optionally)
seed them as `KeywordGroup` rows.

This is the "Layer 2" companion to `fit_keywords`. Where `fit_keywords` extracts
concrete, highlightable per-document keywords (Layer 1), this command groups
those keywords into the broader concepts that recur *across the data* — a
synonym/concept bundle that the cloud's group bar ORs over. It clusters the
keyword vocabulary, not the documents (that's the job BERTopic used to do, and
why document-topics were the wrong tool: a piece of evidence touches several
themes, but a document cluster forces exactly one).

How a keyword is embedded — the key idea. A bare label string ("medien") embeds
ambiguously and only captures the term's dictionary sense. Instead each keyword
is embedded by the *centroid of the documents it was extracted from* (its
contextual embedding), optionally blended with the label embedding. That grounds
the grouping in how the term is actually used in this corpus, disambiguates short
labels, and merges terms used in the same contexts — which is exactly the
"relevant across the data" signal we want. The document embeddings are reused
from the `fit_keywords` cache, so this is nearly free; only uncached documents
are re-encoded.

Grouping uses agglomerative clustering with a cosine *distance threshold* (not a
fixed cluster count), so concepts form at their own natural granularity and you
tune one interpretable knob.

Read-only by default (prints candidate groups). With --apply it CREATES the
groups as `KeywordGroup` rows and assigns each ungrouped, enabled member,
non-destructively: existing groups and already-grouped/disabled keywords are
never touched, so curation in the admin always survives. Refine the seeded
groups there.

    python manage.py group_keywords                      # dry run, default knobs
    python manage.py group_keywords --distance-threshold 0.45
    python manage.py group_keywords --n-clusters 25      # fixed count instead
    python manage.py group_keywords --apply              # seed the groups

Once groups exist, --suggest-members switches from *seeding* to *growing* them:
it ranks each ungrouped keyword against the existing group centroids (the mean of
each group's current members in the same embedding space) and offers the nearest
ones for assignment. This is the curated-theme-anchored way to pull in relevant
keywords a curator hasn't placed yet — relevance is "close to a theme a human
already blessed", not a global score, so off-topic noise (near no theme) is never
offered. We arrived here after a salience-based rescue was tried and rejected:
salience measures how representative a keyword is of its *document*, which turned
out to rank rare noise highly; proximity to a curated theme is the signal that
actually separates relevant from junk. Use --max-df to focus on the long tail.

    python manage.py group_keywords --suggest-members            # dry run
    python manage.py group_keywords --suggest-members --max-df 5 # focus the tail
    python manage.py group_keywords --suggest-members --apply    # assign them

Requires: sentence-transformers, scikit-learn, numpy (the fit_keywords stack).
"""

import hashlib
import os
from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from froide_evidencecollection.management.commands.fit_keywords import (
    DEFAULT_EMBEDDINGS_CACHE,
)
from froide_evidencecollection.models import Evidence, Keyword, KeywordGroup


def _evidence_queryset():
    """Evidence with the relations `Evidence.topic_text` touches prefetched, so
    assembling each document's text doesn't fan out to per-row queries. Mirrors
    the queryset `fit_keywords` builds, so the text (and thus its hash, the cache
    key) matches the cached document embeddings exactly."""
    return (
        Evidence.objects.all()
        .select_related(
            "social_media_post__account",
            "social_media_post__redistributes__account",
            "social_media_post__redistributes__redistributes__account",
        )
        .prefetch_related(
            "social_media_post__images",
            "social_media_post__videos__excerpts",
            "social_media_post__redistributes__images",
            "social_media_post__redistributes__videos__excerpts",
            "social_media_post__redistributes__redistributes__images",
            "social_media_post__redistributes__redistributes__videos__excerpts",
            "mentions__category",
        )
        .order_by("pk")
    )


class Command(BaseCommand):
    help = (
        "Cluster fitted keywords into broad themes (contextual embeddings + "
        "cosine distance threshold) and optionally seed them as KeywordGroups."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--distance-threshold",
            type=float,
            default=0.5,
            help=(
                "Cosine distance at which to stop merging clusters: larger = "
                "fewer, broader groups; smaller = more, tighter groups. Ignored "
                "if --n-clusters is given (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--n-clusters",
            type=int,
            default=None,
            help=(
                "Force exactly this many groups instead of using "
                "--distance-threshold (default: off)."
            ),
        )
        parser.add_argument(
            "--label-weight",
            type=float,
            default=0.3,
            help=(
                "Blend between the keyword's contextual (document-centroid) "
                "embedding and its bare label embedding: 0.0 = pure context "
                "(group by how terms are used), 1.0 = pure label (group by term "
                "meaning alone). The default leans on context while letting the "
                "label keep clear synonyms together (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--min-group-size",
            type=int,
            default=2,
            help=(
                "Skip groups with fewer than this many keywords — a singleton "
                "adds no grouping (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--min-coverage",
            type=int,
            default=0,
            help=(
                "Skip groups covering fewer than this many pieces of evidence "
                "(union over members), so tiny themes don't clutter "
                "(default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--embedding-model",
            default="paraphrase-multilingual-MiniLM-L12-v2",
            help="sentence-transformers model (default matches fit_keywords).",
        )
        parser.add_argument(
            "--max-seq-length",
            type=int,
            default=256,
            help=(
                "Token window for encoding any uncached documents; must match "
                "the fit_keywords run to reuse its cache (default: 256)."
            ),
        )
        parser.add_argument(
            "--embeddings-cache",
            default=None,
            help=(
                "Path to the fit_keywords .npz embeddings cache to reuse for "
                "document vectors. Defaults to the same gitignored project file "
                "fit_keywords writes; pass --no-embeddings-cache to always "
                "re-encode."
            ),
        )
        parser.add_argument(
            "--no-embeddings-cache",
            action="store_true",
            help="Ignore the cache and re-encode every linked document.",
        )
        parser.add_argument(
            "--suggest-members",
            action="store_true",
            help=(
                "Switch from seeding new groups to growing existing ones: rank "
                "each ungrouped keyword against the current KeywordGroup "
                "centroids and offer the nearest as candidate members. Overrides "
                "the clustering knobs above."
            ),
        )
        parser.add_argument(
            "--min-similarity",
            type=float,
            default=0.5,
            help=(
                "In --suggest-members, only offer a keyword whose cosine "
                "similarity to its nearest group centroid is at least this "
                "(default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--top-per-group",
            type=int,
            default=10,
            help=(
                "In --suggest-members, cap how many candidate members to offer "
                "per group, strongest first (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--max-df",
            type=int,
            default=None,
            help=(
                "In --suggest-members, only consider ungrouped keywords with at "
                "most this document frequency — focuses the offer on the rarer "
                "long tail (default: no limit)."
            ),
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help=(
                "Persist what the dry run prints. In the default (clustering) "
                "mode: CREATE the groups as KeywordGroup rows and assign each "
                "ungrouped, enabled member. In --suggest-members mode: assign "
                "each suggested keyword to its nearest group. Either way only "
                "ungrouped, enabled keywords are touched, so existing curation "
                "survives. Without this flag, a dry run that only prints."
            ),
        )
        parser.add_argument(
            "--label-prefix",
            default="",
            help=(
                "Prefix for created group labels with --apply (e.g. 'auto: ') so "
                "seeded groups are easy to spot and edit in the admin."
            ),
        )

    def handle(self, *args, **options):
        try:
            import numpy as np
            from sentence_transformers import SentenceTransformer
            from sklearn.cluster import AgglomerativeClustering
        except ImportError as exc:
            raise CommandError(
                "Missing dependency: %s. Install with `uv pip install keybert`."
                % exc.name
            ) from exc

        # Only enabled keywords are themed: a curator disabled the rest as noise,
        # so they shouldn't shape clusters or coverage.
        keywords = list(
            Keyword.objects.filter(enabled=True)
            .order_by("lemma")
            .values("pk", "lemma", "label", "group_id")
        )
        if not keywords:
            raise CommandError("No enabled keywords found — run fit_keywords first.")
        col = {kw["lemma"]: i for i, kw in enumerate(keywords)}
        labels = [kw["label"] for kw in keywords]
        n_kw = len(keywords)

        # Per-evidence keyword index from the through table (one query), and its
        # inverse (keyword -> evidence) for coverage. Restricted to the enabled
        # keywords above via `col`.
        Through = Evidence.keywords.through
        pairs = Through.objects.values_list("evidence_id", "keyword__lemma")
        ev_kw: dict[int, set[int]] = {}
        kw_evs: list[set[int]] = [set() for _ in range(n_kw)]
        for ev_id, lemma in pairs.iterator(chunk_size=5000):
            j = col.get(lemma)
            if j is not None:
                ev_kw.setdefault(ev_id, set()).add(j)
                kw_evs[j].add(ev_id)
        if not ev_kw:
            raise CommandError(
                "No evidence-keyword links found — run fit_keywords first."
            )
        df = np.array([len(s) for s in kw_evs], dtype=float)

        embedder = SentenceTransformer(options["embedding_model"])
        embedder.max_seq_length = options["max_seq_length"]

        # ── Label embeddings (cheap: a few hundred short strings) ─────────────
        self.stdout.write(f"Embedding {n_kw} keyword labels…")
        label_emb = np.asarray(
            embedder.encode(labels, show_progress_bar=True), dtype=float
        )
        dim = label_emb.shape[1]

        # ── Contextual embeddings: centroid of each keyword's documents ───────
        # Reuse the fit_keywords document-embedding cache (text-hash -> vector);
        # encode only documents missing from it.
        cache_path = self._resolve_cache(options)
        doc_cache = self._load_doc_cache(
            cache_path, options["embedding_model"], options["max_seq_length"]
        )
        ev_vec = self._document_vectors(ev_kw.keys(), doc_cache, embedder, np)

        context = np.zeros((n_kw, dim), dtype=float)
        counts = np.zeros(n_kw, dtype=float)
        for ev_id, idxs in ev_kw.items():
            v = ev_vec.get(ev_id)
            if v is None:
                continue
            for j in idxs:
                context[j] += v
                counts[j] += 1.0
        has_ctx = counts > 0
        context[has_ctx] /= counts[has_ctx, None]

        # ── Blend context + label, then unit-normalise for cosine clustering ──
        def _unit(m):
            return m / np.clip(np.linalg.norm(m, axis=1, keepdims=True), 1e-9, None)

        label_unit = _unit(label_emb)
        ctx_unit = np.zeros_like(label_unit)
        ctx_unit[has_ctx] = _unit(context[has_ctx])
        w = options["label_weight"]
        kw_emb = (1.0 - w) * ctx_unit + w * label_unit
        # Keywords with no linked documents (e.g. stale curated rows, df 0) have
        # no context — fall back to the label embedding alone.
        kw_emb[~has_ctx] = label_unit[~has_ctx]
        kw_emb = _unit(kw_emb)
        self.stdout.write(
            f"  {int(has_ctx.sum())}/{n_kw} keywords embedded with document "
            f"context (label-weight={w})."
        )

        # Member-suggestion mode reuses the embeddings but ranks ungrouped
        # keywords against existing group centroids instead of clustering anew.
        if options["suggest_members"]:
            self._suggest_members(keywords, kw_emb, df, options)
            return

        # ── Cluster ───────────────────────────────────────────────────────────
        if options["n_clusters"]:
            n_clusters = min(options["n_clusters"], n_kw)
            clusterer = AgglomerativeClustering(
                n_clusters=n_clusters, metric="cosine", linkage="average"
            )
            self.stdout.write(f"Clustering into {n_clusters} groups…")
        else:
            clusterer = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=options["distance_threshold"],
                metric="cosine",
                linkage="average",
            )
            self.stdout.write(
                f"Clustering at cosine distance threshold "
                f"{options['distance_threshold']}…"
            )
        group_of = clusterer.fit_predict(kw_emb)

        members: dict[int, list[int]] = {}
        for i, g in enumerate(group_of):
            members.setdefault(int(g), []).append(i)

        def coverage(idxs):
            covered: set[int] = set()
            for j in idxs:
                covered |= kw_evs[j]
            return len(covered)

        def suggested_label(idxs):
            return " / ".join(labels[i] for i in idxs[:3])

        # Keep multi-keyword groups that clear the coverage floor; order by
        # evidence coverage (biggest themes first), members by df within.
        min_size = options["min_group_size"]
        min_cov = options["min_coverage"]
        rows = []
        for idxs in members.values():
            if len(idxs) < min_size:
                continue
            idxs = sorted(idxs, key=lambda i: -df[i])
            cov = coverage(idxs)
            if cov >= min_cov:
                rows.append((cov, idxs))
        rows.sort(key=lambda r: -r[0])

        self.stdout.write(
            f"\n=== {len(rows)} candidate keyword groups "
            f"(by evidence coverage) ===\n"
        )
        for cov, idxs in rows:
            self.stdout.write(
                f"▸ {suggested_label(idxs)}  —  {cov} evidence, "
                f"{len(idxs)} keywords"
            )
            terms = ", ".join(f"{labels[i]}({int(df[i])})" for i in idxs)
            self.stdout.write(f"    {terms}\n")

        if not options["apply"]:
            self.stdout.write(
                "(dry run — pass --apply to create these as KeywordGroups and "
                "assign their ungrouped, enabled keywords.)"
            )
            return

        # ── Apply: seed KeywordGroups, non-destructively ──────────────────────
        # Only assign keywords that are enabled and not already in a group, so
        # existing curation is never clobbered. A group with no assignable
        # members (all already grouped) is skipped.
        prefix = options["label_prefix"]
        created = 0
        assigned = 0
        with transaction.atomic():
            for _cov, idxs in rows:
                member_pks = [keywords[i]["pk"] for i in idxs]
                assignable = list(
                    Keyword.objects.filter(
                        pk__in=member_pks, enabled=True, group__isnull=True
                    ).values_list("pk", flat=True)
                )
                if not assignable:
                    continue
                group = KeywordGroup.objects.create(
                    label=(prefix + suggested_label(idxs))[:100]
                )
                Keyword.objects.filter(pk__in=assignable).update(group=group)
                created += 1
                assigned += len(assignable)
        self.stdout.write(
            self.style.SUCCESS(
                f"\nApplied: created {created} keyword groups, assigned "
                f"{assigned} keywords. Refine them in the admin."
            )
        )

    def _suggest_members(self, keywords, kw_emb, df, options):
        """Offer ungrouped keywords as members of existing KeywordGroups by
        embedding proximity to each group's centroid — the curated-theme-anchored
        rescue. A keyword is "relevant" iff it sits near a theme a human already
        blessed, so off-topic noise (near no theme) is never offered. Read-only
        unless --apply, which assigns each suggested keyword to its nearest group
        (re-checking ungrouped+enabled at write time, so concurrent admin
        curation is never overwritten)."""
        import numpy as np

        def unit(m):
            return m / np.clip(np.linalg.norm(m, axis=-1, keepdims=True), 1e-9, None)

        # Group -> indices of its current (enabled) members; the rest are the
        # ungrouped candidates we might place.
        group_members: dict[int, list[int]] = {}
        candidates: list[int] = []
        for i, kw in enumerate(keywords):
            if kw["group_id"] is not None:
                group_members.setdefault(kw["group_id"], []).append(i)
            else:
                candidates.append(i)
        if not group_members:
            raise CommandError(
                "No keyword groups with enabled members to suggest into. Seed "
                "groups first (run without --suggest-members), then curate."
            )
        max_df = options["max_df"]
        if max_df:
            candidates = [i for i in candidates if df[i] <= max_df]
        if not candidates:
            self.stdout.write(
                "No ungrouped keywords to suggest (tighten/loosen --max-df)."
            )
            return

        # Centroid per group (unit-normalised mean of its members), then the
        # cosine similarity of every candidate to every centroid in one matmul.
        group_ids = sorted(group_members)
        centroids = unit(
            np.vstack([kw_emb[group_members[g]].mean(axis=0) for g in group_ids])
        )
        sims = kw_emb[candidates] @ centroids.T  # (n_candidates, n_groups)
        best_col = sims.argmax(axis=1)
        best_sim = sims.max(axis=1)

        min_sim = options["min_similarity"]
        # group-column -> [(candidate keyword index, similarity)], best group only.
        by_group: dict[int, list[tuple[int, float]]] = defaultdict(list)
        for r, ci in enumerate(candidates):
            if best_sim[r] >= min_sim:
                by_group[int(best_col[r])].append((ci, float(best_sim[r])))

        group_labels = dict(
            KeywordGroup.objects.filter(pk__in=group_ids).values_list("pk", "label")
        )
        top = options["top_per_group"]
        self.stdout.write(
            f"\n=== Member suggestions for {len(group_members)} groups "
            f"(min similarity {min_sim}) ===\n"
        )
        # Groups that drew the most suggestions first.
        pks_by_group: dict[int, list[int]] = {}
        n_sugg = 0
        for gcol, picks in sorted(by_group.items(), key=lambda kv: -len(kv[1])):
            gid = group_ids[gcol]
            picks = sorted(picks, key=lambda p: -p[1])[:top]
            self.stdout.write(f"▸ {group_labels.get(gid, gid)}  (+{len(picks)})")
            for ci, sim in picks:
                kw = keywords[ci]
                self.stdout.write(
                    f"    {kw['label'][:30]:30} df={int(df[ci]):>3} sim={sim:.3f}"
                )
            pks_by_group[gid] = [keywords[ci]["pk"] for ci, _ in picks]
            n_sugg += len(picks)
            self.stdout.write("")

        if n_sugg == 0:
            self.stdout.write("No suggestions cleared the similarity threshold.")
            return
        if not options["apply"]:
            self.stdout.write(
                f"(dry run — {n_sugg} suggestions; pass --apply to assign each to "
                "its nearest group.)"
            )
            return

        with transaction.atomic():
            assigned = 0
            for gid, pks in pks_by_group.items():
                assigned += Keyword.objects.filter(
                    pk__in=pks, enabled=True, group__isnull=True
                ).update(group_id=gid)
        self.stdout.write(
            self.style.SUCCESS(f"\nApplied: assigned {assigned} keywords to groups.")
        )

    def _resolve_cache(self, options):
        """The effective embeddings-cache path: None when disabled, else the
        explicit path or the fit_keywords default (normalised to .npz)."""
        if options["no_embeddings_cache"]:
            return None
        path = options["embeddings_cache"] or DEFAULT_EMBEDDINGS_CACHE
        if path and not path.endswith(".npz"):
            path += ".npz"
        return path

    def _load_doc_cache(self, cache_path, model_name, max_seq_length):
        """Load the document-embedding cache (text-hash -> vector) written by
        fit_keywords. Returns {} when caching is off, the file is missing/
        unreadable, or it was written with a different model / max-seq-length
        (a mismatched vector means nothing under a different encoder)."""
        import numpy as np

        if not cache_path or not os.path.exists(cache_path):
            return {}
        try:
            data = np.load(cache_path, allow_pickle=False)
            if (
                str(data["model"]) != model_name
                or int(data["max_seq_length"]) != max_seq_length
            ):
                self.stdout.write(
                    "  embeddings cache ignored (model or max-seq-length "
                    "differs); re-encoding documents."
                )
                return {}
            return dict(
                zip(data["doc_keys"].tolist(), data["doc_vectors"], strict=False)
            )
        except Exception as exc:  # corrupt / old-format / unreadable → re-encode
            self.stdout.write(f"  embeddings cache unreadable ({exc}); re-encoding.")
            return {}

    def _document_vectors(self, ev_ids, doc_cache, embedder, np):
        """Map each linked evidence id to its document embedding, taking cache
        hits (keyed by the same text hash fit_keywords uses) and encoding only
        the misses in one batch. Returns ``{evidence_id: np.ndarray}``."""
        ev_ids = set(ev_ids)
        ev_vec: dict[int, "np.ndarray"] = {}
        miss_ids: list[int] = []
        miss_texts: list[str] = []
        qs = _evidence_queryset().filter(pk__in=ev_ids)
        for ev in qs.iterator(chunk_size=500):
            text = ev.topic_text
            h = hashlib.sha1(text.encode("utf-8")).hexdigest()
            v = doc_cache.get(h)
            if v is not None:
                ev_vec[ev.pk] = np.asarray(v, dtype=float)
            else:
                miss_ids.append(ev.pk)
                miss_texts.append(text)
        if miss_texts:
            self.stdout.write(
                f"Encoding {len(miss_texts)} uncached documents "
                f"({len(ev_vec)} from cache)…"
            )
            vecs = np.asarray(
                embedder.encode(miss_texts, show_progress_bar=True), dtype=float
            )
            for i, ev_id in enumerate(miss_ids):
                ev_vec[ev_id] = vecs[i]
        else:
            self.stdout.write(f"  {len(ev_vec)} document vectors from cache.")
        return ev_vec

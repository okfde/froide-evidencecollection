"""Read-only prototype: compare ways to rank keyword facets by "interestingness"
instead of raw frequency.

Run from the host project:
    python manage.py cluster_keywords_prototype                 # global rankings
    python manage.py cluster_keywords_prototype --topic 12      # ranking within a slice
    python manage.py cluster_keywords_prototype --topic Impf     # slice by label substring
    python manage.py cluster_keywords_prototype --groups 20      # candidate group chips
    python manage.py cluster_keywords_prototype --groups 20 --apply   # seed them

Reads the existing Evidence↔Keyword index (built by `fit_post_topics`) and
prints, side by side, the keyword orderings produced by several scoring schemes.

  GROUPS (--groups N): cluster keywords into N coarse, embedding-similarity
  groups (synonyms land together) and print each as a candidate starting chip —
  a suggested label, evidence coverage, and its *full* membership — to inspire
  the keyword groups you curate. Read-only by default; with --apply it creates
  the groups as KeywordGroup rows and assigns each ungrouped, enabled member,
  leaving existing curation untouched (you then refine in the admin).

  GLOBAL (no --topic):
    frequency       — raw corpus document frequency (what the cloud uses today)
    idf-weighted    — df × log(N/df): damps ubiquitous words
    emb-specificity — distance of the keyword embedding from the corpus centroid
                      (semantic analogue of IDF: generic words sit near centre)

  SLICE (--topic given): how keywords rank *within* the filtered evidence:
    freq-in-slice   — raw count inside the slice (today's behaviour, for contrast)
    log-odds(z)     — log-odds-ratio with informative Dirichlet prior (Monroe et
                      al.); the count-based keyness now wired into _build_facets
    emb-keyness     — cosine of the keyword embedding with the slice's distinctive
                      semantic *direction* (slice centroid − corpus centroid);
                      robust to sparsity/synonymy, count-free
    logodds+MMR     — log-odds(z) re-ranked with Maximal Marginal Relevance over
                      the keyword embeddings, so near-synonyms are spread out
                      instead of clumping at the top (the diversity dial)

Embeddings use the same SentenceTransformer as fit_post_topics, computed on the
keyword labels (a few hundred short strings — cheap). Read-only except for
`--groups --apply`, which seeds KeywordGroup rows (additively, non-destructively).

Requires bertopic's stack (sentence-transformers, numpy, scikit-learn).
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from froide_evidencecollection.models import Evidence, Keyword, KeywordGroup


class Command(BaseCommand):
    help = (
        "Prototype: compare keyword-facet ranking schemes (frequency / keyness / "
        "embedding keyness / MMR). Read-only, prints only."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--topic",
            default="",
            help=(
                "Restrict to a slice: a Topic PK, or a substring matched against "
                "Topic.label. Without it, prints the global rankings."
            ),
        )
        parser.add_argument(
            "--top",
            type=int,
            default=30,
            help="How many keywords to show per ranking (default: %(default)s).",
        )
        parser.add_argument(
            "--embedding-model",
            default="paraphrase-multilingual-MiniLM-L12-v2",
            help="sentence-transformers model (default matches fit_post_topics).",
        )
        parser.add_argument(
            "--groups",
            type=int,
            default=0,
            help=(
                "Coarse-grouping mode: cluster keywords into this many groups by "
                "embedding similarity and print them as candidate starting chips "
                "(try ~15-25). Overrides the ranking comparison."
            ),
        )
        parser.add_argument(
            "--min-coverage",
            type=int,
            default=0,
            help=(
                "In --groups mode, skip groups covering fewer than this many "
                "pieces of evidence (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help=(
                "In --groups mode, CREATE the printed groups as KeywordGroup "
                "rows and assign each ungrouped, enabled member keyword to them. "
                "Existing groups and already-assigned/disabled keywords are left "
                "untouched, so it only seeds — refine in the admin afterwards. "
                "Without this flag, --groups is a dry run that only prints."
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
        parser.add_argument(
            "--mmr-lambda",
            type=float,
            default=0.6,
            help=(
                "MMR trade-off: 1.0 = pure relevance, 0.0 = pure diversity "
                "(default: %(default)s)."
            ),
        )

    def handle(self, *args, **options):
        try:
            import numpy as np
            from sentence_transformers import SentenceTransformer
            from sklearn.cluster import AgglomerativeClustering
        except ImportError as exc:
            raise CommandError(
                "Missing dependency: %s. Install with `uv pip install bertopic`."
                % exc.name
            ) from exc

        top = options["top"]

        # lemma -> index, plus display label.
        keywords = list(
            Keyword.objects.order_by("lemma").values("pk", "lemma", "label")
        )
        if not keywords:
            raise CommandError("No keywords found — run fit_post_topics first.")
        col = {kw["lemma"]: i for i, kw in enumerate(keywords)}
        labels = [kw["label"] for kw in keywords]
        n_kw = len(keywords)

        # Per-evidence keyword index from the through table (one query).
        Through = Evidence.keywords.through
        pairs = Through.objects.values_list("evidence_id", "keyword__lemma")
        ev_kw: dict[int, set[int]] = {}
        for ev_id, lemma in pairs.iterator(chunk_size=5000):
            j = col.get(lemma)
            if j is not None:
                ev_kw.setdefault(ev_id, set()).add(j)

        # N = documents in the keyword space; df = per-keyword document freq.
        N = len(ev_kw)
        df = np.zeros(n_kw, dtype=float)
        for idxs in ev_kw.values():
            for j in idxs:
                df[j] += 1.0
        self.stdout.write(
            f"{n_kw} keywords across {N} pieces of evidence "
            f"(df min/median/max: {int(df.min())}/{int(np.median(df))}/{int(df.max())})."
        )

        # Keyword embeddings (unit-normalised, so dot products are cosines).
        self.stdout.write(
            f"Embedding {n_kw} keywords with {options['embedding_model']}…"
        )
        embedder = SentenceTransformer(options["embedding_model"])
        emb = embedder.encode(labels, show_progress_bar=True).astype(float)
        unit = emb / np.clip(np.linalg.norm(emb, axis=1, keepdims=True), 1e-9, None)

        def fmt_rows(order, value_of, value_fmt):
            """(label, formatted-score) for the indices in `order`, top first."""
            return [(labels[i], value_fmt(value_of(i))) for i in order[:top]]

        if options["groups"]:
            # ── Coarse-grouping mode: candidate starting chips ───────────────
            # Cluster keywords into a small fixed number of groups by embedding
            # similarity (synonyms land together), then print each group as a
            # would-be starting chip: an auto-label, how many pieces of evidence
            # it covers (union of its members — the OR-filter reach), and its
            # member terms. This is what the empty-state chip row would show.
            n_groups = min(options["groups"], n_kw)
            group_of = AgglomerativeClustering(
                n_clusters=n_groups, metric="cosine", linkage="average"
            ).fit_predict(emb)

            # Invert the index so we can count evidence coverage per group.
            kw_evs: list[set[int]] = [set() for _ in range(n_kw)]
            for ev_id, idxs in ev_kw.items():
                for j in idxs:
                    kw_evs[j].add(ev_id)

            members: dict[int, list[int]] = {}
            for i, g in enumerate(group_of):
                members.setdefault(int(g), []).append(i)

            def coverage(idxs):
                covered: set[int] = set()
                for j in idxs:
                    covered |= kw_evs[j]
                return len(covered)

            # Order groups by evidence coverage — the biggest starting themes
            # first, which is how the empty-state chips would be sorted. Drop
            # groups under --min-coverage so tiny clusters don't clutter.
            min_cov = options["min_coverage"]
            rows = []
            for idxs in members.values():
                idxs = sorted(idxs, key=lambda i: -df[i])
                cov = coverage(idxs)
                if cov >= min_cov:
                    rows.append((cov, idxs))
            rows.sort(key=lambda r: -r[0])

            def suggested_label(idxs):
                return " / ".join(labels[i] for i in idxs[:3])

            self.stdout.write(
                f"\n=== {len(rows)} coarse keyword groups (candidate starting "
                f"chips, by evidence coverage) ===\n"
            )
            # Full membership (not a preview): this is the set you'd assign to a
            # group, so print every member with its df.
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

            # ── Apply: seed KeywordGroups, non-destructively ─────────────────
            # Only assign keywords that are enabled and not already in a group,
            # so existing curation is never clobbered. A group with no assignable
            # members (all already grouped/disabled) is skipped.
            prefix = options["label_prefix"]
            created = 0
            assigned = 0
            with transaction.atomic():
                for _cov, idxs in rows:
                    member_pks = [keywords[i]["pk"] for i in idxs]
                    assignable = list(
                        Keyword.objects.filter(
                            pk__in=member_pks, enabled=True, group__isnull=True
                        )
                    )
                    if not assignable:
                        continue
                    group = KeywordGroup.objects.create(
                        label=(prefix + suggested_label(idxs))[:100]
                    )
                    Keyword.objects.filter(pk__in=[k.pk for k in assignable]).update(
                        group=group
                    )
                    created += 1
                    assigned += len(assignable)
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nApplied: created {created} keyword groups, assigned "
                    f"{assigned} keywords. Refine them in the admin."
                )
            )
            return

        if not options["topic"]:
            # ── Global rankings ──────────────────────────────────────────────
            idf = np.log(N / np.clip(df, 1, None))
            idf_weighted = df * idf
            # Embedding specificity: distance from the df-weighted corpus
            # centroid. Generic words cluster near the centre (low score).
            corpus_centroid = (df[:, None] * unit).sum(0) / df.sum()
            cc = corpus_centroid / max(np.linalg.norm(corpus_centroid), 1e-9)
            emb_spec = 1.0 - unit @ cc
            cols = [
                (
                    "frequency",
                    fmt_rows(np.argsort(-df), lambda i: df[i], lambda v: f"{int(v)}"),
                ),
                (
                    "idf-weighted",
                    fmt_rows(
                        np.argsort(-idf_weighted),
                        lambda i: idf_weighted[i],
                        lambda v: f"{v:.0f}",
                    ),
                ),
                (
                    "emb-specificity",
                    fmt_rows(
                        np.argsort(-emb_spec),
                        lambda i: emb_spec[i],
                        lambda v: f"{v:.2f}",
                    ),
                ),
            ]
            self.stdout.write("\n=== Global keyword rankings ===\n")
            self._print_table(cols, top)
            return

        # ── Resolve the slice ───────────────────────────────────────────────
        topic_arg = options["topic"].strip()
        ev_qs = Evidence.objects.filter(topic__isnull=False)
        if topic_arg.isdigit():
            ev_qs = ev_qs.filter(topic_id=int(topic_arg))
            slice_desc = f"topic_id={topic_arg}"
        else:
            ev_qs = ev_qs.filter(topic__label__icontains=topic_arg)
            slice_desc = f"topic label ~ {topic_arg!r}"
        slice_ids = set(ev_qs.values_list("pk", flat=True)) & ev_kw.keys()
        if not slice_ids:
            raise CommandError(f"No fitted evidence matched the slice ({slice_desc}).")

        # Per-keyword count inside the slice.
        a = np.zeros(n_kw, dtype=float)
        for ev_id in slice_ids:
            for j in ev_kw[ev_id]:
                a[j] += 1.0
        n_slice = float(len(slice_ids))
        self.stdout.write(
            f"\nSlice: {slice_desc} — {int(n_slice)} pieces of evidence "
            f"({int(n_slice / N * 100)}% of corpus).\n"
        )

        present = np.where(a > 0)[0]

        # ── Count-based keyness: log-odds with informative Dirichlet prior ───
        b = df - a  # occurrences outside the slice
        n_i = a.sum()
        n_j = df.sum() - n_i
        alpha = df
        a0 = df.sum()
        with np.errstate(divide="ignore", invalid="ignore"):
            delta = np.log((a + alpha) / (n_i + a0 - a - alpha)) - np.log(
                (b + alpha) / (n_j + a0 - b - alpha)
            )
            var = 1.0 / (a + alpha) + 1.0 / (b + alpha)
        z = delta / np.sqrt(var)

        # ── Embedding keyness: alignment with the slice's distinctive direction ─
        slice_centroid = (a[:, None] * unit).sum(0) / max(a.sum(), 1e-9)
        corpus_centroid = (df[:, None] * unit).sum(0) / df.sum()
        sc = slice_centroid / max(np.linalg.norm(slice_centroid), 1e-9)
        cc = corpus_centroid / max(np.linalg.norm(corpus_centroid), 1e-9)
        direction = sc - cc
        direction = direction / max(np.linalg.norm(direction), 1e-9)
        emb_key = unit @ direction

        # ── MMR re-rank of the log-odds(z) ranking over keyword embeddings ───
        mmr_order = self._mmr(present, z, unit, top, options["mmr_lambda"], np)

        def order_by(score):
            return present[np.argsort(-score[present])]

        cols = [
            (
                "freq-in-slice",
                fmt_rows(order_by(a), lambda i: a[i], lambda v: f"{int(v)}"),
            ),
            (
                "log-odds(z)",
                fmt_rows(order_by(z), lambda i: z[i], lambda v: f"{v:.1f}"),
            ),
            (
                "emb-keyness",
                fmt_rows(order_by(emb_key), lambda i: emb_key[i], lambda v: f"{v:.2f}"),
            ),
            (
                "logodds+MMR",
                fmt_rows(mmr_order, lambda i: z[i], lambda v: f"{v:.1f}"),
            ),
        ]
        self.stdout.write("=== Slice keyword rankings (most distinctive first) ===\n")
        self._print_table(cols, top)

    @staticmethod
    def _mmr(present, relevance, unit, k, lam, np):
        """Maximal Marginal Relevance over `present` keyword indices: greedily
        pick the keyword maximising lam·rel − (1−lam)·max cosine-sim to already
        picked, so near-synonyms are spread out. `relevance` is min-max
        normalised over the pool so it's comparable to the cosine penalty."""
        pool = list(present[np.argsort(-relevance[present])])
        if not pool:
            return np.array([], dtype=int)
        rel = relevance[pool]
        lo, hi = rel.min(), rel.max()
        rel_norm = {i: (relevance[i] - lo) / (hi - lo or 1.0) for i in pool}
        selected = []
        sims = unit @ unit.T
        while pool and len(selected) < k:
            if not selected:
                best = pool[0]
            else:
                best = max(
                    pool,
                    key=lambda i: lam * rel_norm[i]
                    - (1 - lam) * max(sims[i, j] for j in selected),
                )
            selected.append(best)
            pool.remove(best)
        return np.array(selected, dtype=int)

    def _print_table(self, cols, top):
        """Print ranking columns side by side: each cell is `label  score`."""
        colw = 26
        headers = "  ".join(f"{h[:colw]:<{colw}}" for h, _ in cols)
        self.stdout.write(headers)
        self.stdout.write("  ".join("-" * colw for _ in cols))
        for r in range(top):
            cells = []
            for _h, rows in cols:
                if r < len(rows):
                    lbl, val = rows[r]
                    cell = f"{lbl[:17]:<17} {val:>7}"
                else:
                    cell = ""
                cells.append(f"{cell[:colw]:<{colw}}")
            self.stdout.write("  ".join(cells).rstrip())

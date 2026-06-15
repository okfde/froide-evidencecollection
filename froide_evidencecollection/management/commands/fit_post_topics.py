"""Fit a BERTopic model over Evidence text, persist flat Topic rows with
per-topic keywords, update each evidence's topic FK + 2D coordinates for the
topic cloud view, and build the evidence ↔ keyword index that drives the
cloud's keyword facets.

Each evidence's input text is `Evidence.topic_text` — the source text
(post body, transcription, redistributed content, document text, …) assembled
highest-signal-first, since the embedding model truncates to a token window.

Keyword facets: the candidate vocabulary is the union of the leaf topics'
c-TF-IDF keywords, lemmatised (German) and deduplicated. Each evidence is then
linked to the keywords whose lemma actually occurs in its text, so a facet
selection narrows to evidence that genuinely contains the word rather than to
evidence merely sharing a cluster.

Run manually (or via cron); the topic cloud view reads the cached numbers.

Requires: bertopic, sentence-transformers, umap-learn, hdbscan, numpy,
scikit-learn, simplemma. Install with `uv pip install bertopic simplemma`.
"""

import argparse
import re

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from froide_evidencecollection.models import Evidence, Keyword, Topic

# Curated German stopword list, expanded with social-media filler. Used as
# the c-TF-IDF vocabulary filter so topic labels surface content words instead
# of "der | die | und". Inlined to avoid pulling in nltk/spacy just for this.
GERMAN_STOPWORDS = """
á a ab aber ach acht achte achten achter achtes ag äh ähm alle allein allem allen
aller allerdings alles allgemeinen als also am an andere anderen anderem andern
anders auch auf aufgrund aus ausser außer ausserdem außerdem

bald bedeuten bedeutet bei beide beiden beim beispiel bekannt bereits besonders besser besten bin
bis bisher bisschen bist

da dabei dadurch dafür dagegen daher dahin dahinter damals damit danach daneben
dank dann daran darauf daraus darf darfst darin darüber darum darunter das
dasein daselbst dass daß dasselbe davon davor dazu dazwischen dein deine deinem
deiner dem dementsprechend demgegenüber demgemäss demgemäß demselben demzufolge
den denen denn denselben der deren derjenige derjenigen dermassen dermaßen
derselbe derselben des deshalb desselben dessen deswegen dich die diejenige
diejenigen dies diese dieselbe dieselben diesem diesen dieser dieses dir doch
dort drei drin dritte dritten dritter drittes du durch durchaus dürfen dürft
durfte durften

eben ebenso ehrlich eigen eigene eigenen eigener eigenes eigentlich ein einander eine
einem einen einer eines einige einigen einiger einiges einmal einmaleins elf en
ende endlich entweder er erst erste ersten erster erstes es etwa etwas euch

früher fünf fünfte fünften fünfter fünftes für

gab ganz ganze ganzen ganzer ganzes gar gedurft gegen gegenüber gehabt gehen
geht gekannt gekonnt gemacht gemocht gemusst genau genug gerade gern gesagt geschweige
gewesen gewollt geworden gibt ging gleich gross groß grosse große grossen
großen grosser großer grosses großes gut gute guter gutes

habe haben habt hast hat hatte hätte hatten hätten heisst heißt her heute hier
hin hinter hoch

ich ihm ihn ihnen ihr ihre ihrem ihren ihrer ihres im immer in indem
infolgedessen ins insofern irgend irgendwie ist

ja jahr jahre jahren je jede jedem jeden jeder jedermann jedermanns jedoch
jemand jemandem jemanden jene jenem jenen jener jenes jetzt

kam kann kannst kaum kein keine keinem keinen keiner kleine kleinen kleiner
kleines kommen kommt können könnt konnte könnte konnten kurz

lang lange leicht leider letztendlich lieber los

machen macht machte mag magst mal man manche manchem manchen mancher manches mehr
mein meine meinem meinen meiner meines mich mir mit mittel mochte möchte mochten
mögen möglich mögt morgen muss muß müssen musst müsst musste mussten

na nach nachdem nahm nämlich natürlich ne neben nein neue neuen neun neunte neunten neunter
neuntes nicht nichts nie niemand niemandem niemanden noch nun nur

ob oben oder offen oft ohne

recht rechte rechten rechter rechtes richtig rund

sagen sagt sagte sah satt schlecht schon sechs sechste sechsten sechster sechstes
sehr sei seid seien sein seine seinem seinen seiner seines seit seitdem selbst
selbst sich sie sieben siebente siebenten siebenter siebentes siebte siebten
siebter siebtes sind so sogar solang solche solchem solchen solcher solches soll
sollen sollte sollten sondern sonst sowie sozusagen später statt

tag tage tagen tat teil tel trotzdem tun

über überhaupt übrigens uhr um und uns unser unsere unserer unter

vergangene vergangenen viel viele vielem vielen vielleicht vier vierte vierten
vierter viertes vom von vor

wahr während währenddem währenddessen wann war wäre waren wart warum was wegen
weil weit weiter weitere weiteren weiteres welche welchem welchen welcher
welches wem wen wenig wenige weniger weniges wenigstens wenn wer werde werden
werdet wessen wie wieder will willst wir wird wirklich wirst wo wohl wollen
wollt wollte wollten worden wurde würde wurden würden

zehn zehnte zehnten zehnter zehntes zeit zu zuerst zugleich zum zunächst zur
zurück zusammen zwanzig zwar zwei zweite zweiten zweiter zweites zwischen

co prozent
""".split()

# Number of top keywords to persist per topic (used as both keyword list and
# auto-generated label source).
TOPIC_KEYWORDS_N = 20
# Jaccard threshold for `--keep-labels` to consider an old topic the "same"
# cluster as a new one. Tuned empirically — keyword sets shift but a strong
# overlap is a reliable signal.
KEEP_LABEL_JACCARD_THRESHOLD = 0.4

# Keyword facet index: a candidate keyword (from the union of leaf c-TF-IDF
# terms) only becomes a real facet if its lemma occurs in at least this many
# pieces of evidence — drops one-off matches that would clutter the cloud.
DEFAULT_KEYWORD_MIN_DF = 3
# Token pattern for lemmatising evidence text: word characters incl. German
# umlauts/ß, length >= 2 to mirror the c-TF-IDF vocabulary (CountVectorizer's
# default token pattern also keeps 2+ char tokens). Lower-cased before matching.
_WORD_RE = re.compile(r"[0-9a-zà-öø-ÿ]{2,}", re.IGNORECASE)


def _auto_label(keywords: list[str], n: int = 3) -> str:
    """Default label = first N keywords joined by ' | '. Truncated to fit
    the model field."""
    if not keywords:
        return ""
    return " | ".join(keywords[:n])[:255]


def _make_lemmatizer():
    """Return a cached German lemmatiser ``str -> str`` backed by simplemma.

    Lemmatising is done once here at fit time (never per request); the result
    is the normalised match key stored on each `Keyword`. A small dict cache
    keeps repeated tokens cheap across the whole corpus."""
    try:
        import simplemma
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise CommandError(
            "Missing dependency: simplemma. Install with `uv pip install simplemma`."
        ) from exc

    cache: dict[str, str] = {}

    def lemmatize(token: str) -> str:
        token = token.lower()
        lemma = cache.get(token)
        if lemma is None:
            lemma = simplemma.lemmatize(token, lang="de").lower()
            cache[token] = lemma
        return lemma

    return lemmatize


def _lemma_set(text: str, lemmatize) -> set[str]:
    """Lemmatise every word-token of ``text`` into a set of distinct lemmas."""
    return {lemmatize(m.group(0)) for m in _WORD_RE.finditer(text)}


class Command(BaseCommand):
    help = "Fit BERTopic over Evidence text and store Topic rows + 2D coords."

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
                "truncated and never influences the topic. The default model "
                "ships with 128, but most evidence exceeds that — 256 captures "
                "the typical document; the architectural max is ~512 "
                "(default: 256)."
            ),
        )
        parser.add_argument(
            "--min-topic-size",
            type=int,
            default=10,
            help="HDBSCAN min_cluster_size — smaller = more topics (default: 10).",
        )
        parser.add_argument(
            "--cluster-selection",
            choices=("eom", "leaf"),
            default="leaf",
            help=(
                "HDBSCAN cluster_selection_method. 'leaf' yields many small, "
                "evenly-sized topics; 'eom' (excess of mass) prefers a few "
                "large clusters and can collapse to 2 on dense data "
                "(default: leaf)."
            ),
        )
        parser.add_argument(
            "--umap-neighbors",
            type=int,
            default=15,
            help=(
                "UMAP n_neighbors for the clustering projection — smaller "
                "favours local structure / more topics (default: 15)."
            ),
        )
        parser.add_argument(
            "--reduce-outliers",
            action=argparse.BooleanOptionalAction,
            default=True,
            help=(
                "Reassign HDBSCAN outliers (-1) to their nearest topic by "
                "embedding similarity after the fit, so few/no pieces of "
                "evidence are left unclustered. Use --no-reduce-outliers to "
                "keep the raw outlier cluster (default: enabled)."
            ),
        )
        parser.add_argument(
            "--outlier-threshold",
            type=float,
            default=0.0,
            help=(
                "Minimum cosine similarity for --reduce-outliers to move an "
                "outlier into a topic; outliers below it stay unclustered. "
                "0.0 reassigns all (default: 0.0)."
            ),
        )
        parser.add_argument(
            "--keyword-min-df",
            type=int,
            default=DEFAULT_KEYWORD_MIN_DF,
            help=(
                "A candidate keyword only becomes a facet if its lemma occurs "
                "in at least this many pieces of evidence (default: %(default)s)."
            ),
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
                "Also clear topic FK + coords on evidence that doesn't end up "
                "in the fitted set (e.g. too short, dropped by --limit)."
            ),
        )
        parser.add_argument(
            "--keep-labels",
            action="store_true",
            help=(
                "Carry curated labels/descriptions forward from the previous "
                "fit by matching old → new topics on keyword overlap (Jaccard "
                f">= {KEEP_LABEL_JACCARD_THRESHOLD}). Without this flag, each "
                "refit starts with auto-generated labels."
            ),
        )

    def handle(self, *args, **options):
        try:
            from bertopic import BERTopic
            from hdbscan import HDBSCAN
            from sentence_transformers import SentenceTransformer
            from sklearn.feature_extraction.text import CountVectorizer
            from umap import UMAP
        except ImportError as exc:
            raise CommandError(
                "Missing dependency: %s. Install with `uv pip install bertopic`."
                % exc.name
            ) from exc

        min_len = options["min_text_len"]
        # select_related/prefetch_related cover everything Evidence.topic_text
        # touches: both source kinds, the redistribution chain a post recurses
        # into, and each post's media (a to-many, so prefetched), so iterating
        # doesn't fan out to per-row queries.
        qs = (
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
        if options["limit"]:
            qs = qs[: options["limit"]]

        evidences: list[Evidence] = []
        docs: list[str] = []
        # chunk_size is required for .iterator() once prefetch_related is in
        # play (Django fetches the related rows per chunk).
        for ev in qs.iterator(chunk_size=500):
            text = ev.topic_text
            if len(text) < min_len:
                continue
            evidences.append(ev)
            docs.append(text)

        if len(docs) < options["min_topic_size"] * 2:
            raise CommandError(
                f"Need at least {options['min_topic_size'] * 2} usable pieces of "
                f"evidence to fit topics; got {len(docs)}. Lower --min-topic-size "
                "or import more evidence."
            )

        self.stdout.write(
            f"Embedding {len(docs)} pieces of evidence with "
            f"{options['embedding_model']} (max_seq_length="
            f"{options['max_seq_length']})…"
        )
        embedder = SentenceTransformer(options["embedding_model"])
        # Raise the truncation window past the model's short default so more of
        # each document reaches the encoder (see --max-seq-length).
        embedder.max_seq_length = options["max_seq_length"]
        embeddings = embedder.encode(docs, show_progress_bar=True)

        self.stdout.write("Fitting BERTopic…")
        # German stopword vocabulary so c-TF-IDF labels are content words,
        # not function words. `min_df=2` drops one-off tokens that would
        # otherwise float to the top of small clusters.
        vectorizer = CountVectorizer(
            stop_words=GERMAN_STOPWORDS,
            min_df=2,
            ngram_range=(1, 2),
        )
        # Explicit UMAP + HDBSCAN models instead of BERTopic's defaults so we
        # control clustering. `cluster_selection_method='leaf'` is the key
        # knob: the default 'eom' rolls dense data up into a couple of
        # macro-clusters (it once collapsed this corpus to 2 topics with 0
        # outliers); 'leaf' picks fine-grained, evenly-sized topics instead.
        # random_state makes refits reproducible (and pins UMAP to 1 thread).
        umap_model = UMAP(
            n_components=5,
            n_neighbors=options["umap_neighbors"],
            min_dist=0.0,
            metric="cosine",
            random_state=42,
        )
        hdbscan_model = HDBSCAN(
            min_cluster_size=options["min_topic_size"],
            metric="euclidean",
            cluster_selection_method=options["cluster_selection"],
            prediction_data=True,
        )
        topic_model = BERTopic(
            embedding_model=embedder,
            umap_model=umap_model,
            hdbscan_model=hdbscan_model,
            vectorizer_model=vectorizer,
            calculate_probabilities=False,
            language="multilingual",
            verbose=True,
        )
        topics, _probs = topic_model.fit_transform(docs, embeddings)
        topics = [int(t) for t in topics]

        # Snapshot the raw HDBSCAN assignment before outlier reduction, so we
        # can flag which evidence were genuine outliers (-1) that got moved
        # into a topic below (persisted as Evidence.topic_reassigned).
        raw_topics = list(topics)

        # Fine-grained 'leaf' clustering leaves a large outlier (-1) bucket;
        # reassign those to their nearest topic by embedding similarity so few
        # pieces of evidence stay unclustered, while the topic *cores* keep the
        # clean shape leaf gave them. `threshold` lets genuinely-distant
        # outliers stay -1. update_topics then recomputes c-TF-IDF keywords for
        # the new assignment.
        if options["reduce_outliers"]:
            n_before = sum(1 for t in topics if t == -1)
            topics = topic_model.reduce_outliers(
                docs,
                topics,
                strategy="embeddings",
                embeddings=embeddings,
                threshold=options["outlier_threshold"],
            )
            topics = [int(t) for t in topics]
            topic_model.update_topics(docs, topics=topics, vectorizer_model=vectorizer)
            n_after = sum(1 for t in topics if t == -1)
            n_moved = sum(
                1
                for raw, new in zip(raw_topics, topics, strict=False)
                if raw == -1 and new != -1
            )
            self.stdout.write(
                f"Reduced outliers: {n_before} → {n_after} unclustered "
                f"({n_moved} reassigned to topics)."
            )

        self.stdout.write("Reducing embeddings to 2D for the cloud view…")
        # Separate UMAP-to-2D pass: BERTopic's internal UMAP is 5D for
        # clustering, which is wrong for plotting. Seed for reproducibility
        # so re-runs don't rotate the cloud.
        coords = UMAP(
            n_components=2,
            n_neighbors=15,
            min_dist=0.1,
            metric="cosine",
            random_state=42,
        ).fit_transform(embeddings)

        # Per-leaf keywords (top TOPIC_KEYWORDS_N c-TF-IDF terms, content words
        # only thanks to the stopword vocabulary). Outliers have no meaningful
        # keywords — BERTopic returns "" entries — so we drop empty terms.
        leaf_keywords: dict[int, list[str]] = {}
        leaf_sizes: dict[int, int] = {}
        for tid in set(topics):
            terms = [w for w, _s in topic_model.get_topic(tid) or []]
            leaf_keywords[tid] = [w for w in terms if w][:TOPIC_KEYWORDS_N]
            leaf_sizes[tid] = sum(1 for t in topics if t == tid)

        # Snapshot existing topics for label carry-forward BEFORE we wipe.
        # Stored as (label, description, keyword_set) keyed by old PK; we'll
        # match new leaves against this map by Jaccard overlap of keywords.
        carry: dict[int, tuple[str, str, set[str]]] = {}
        if options["keep_labels"]:
            for old in Topic.objects.exclude(label="").iterator():
                if not old.keywords:
                    continue
                carry[old.pk] = (
                    old.label,
                    old.description,
                    {k.lower() for k in old.keywords},
                )
            self.stdout.write(
                f"Tracking {len(carry)} existing labelled topics for carry-forward."
            )

        # ── Keyword facet index ──────────────────────────────────────────
        # Candidate vocabulary = union of the leaf topics' c-TF-IDF keywords
        # (the outlier's are noise, skipped). Each candidate is lemmatised to
        # its German base form; surface forms collapsing to one lemma merge,
        # the shortest winning as the display label. Then each evidence's text
        # is lemmatised and intersected with the vocabulary, so a keyword links
        # only to evidence that genuinely contains it — that is what makes a
        # facet selection narrow to matching text rather than to a whole
        # cluster. A lemma is kept only if it reaches --keyword-min-df.
        self.stdout.write("Building keyword facet index…")
        lemmatize = _make_lemmatizer()

        candidate_surface: dict[str, str] = {}  # lemma -> display label
        for tid, kws in leaf_keywords.items():
            if tid == -1:
                continue
            for kw in kws:
                lemma = lemmatize(kw)
                if not lemma:
                    continue
                prev = candidate_surface.get(lemma)
                if prev is None or len(kw) < len(prev):
                    candidate_surface[lemma] = kw
        vocab = set(candidate_surface)

        # Per-evidence matched lemmas (restricted to the vocabulary) plus a
        # corpus-wide document frequency so we can drop rare ones.
        ev_lemmas: list[set[str]] = []
        lemma_df: dict[str, int] = {}
        for text in docs:
            matched = _lemma_set(text, lemmatize) & vocab
            ev_lemmas.append(matched)
            for lemma in matched:
                lemma_df[lemma] = lemma_df.get(lemma, 0) + 1

        min_df = options["keyword_min_df"]
        kept_lemmas = {lemma for lemma, df in lemma_df.items() if df >= min_df}
        self.stdout.write(
            f"Keyword index: {len(kept_lemmas)} keywords (of {len(vocab)} "
            f"candidates) with df >= {min_df}."
        )

        now = timezone.now()

        def _carry(new_keywords: list[str]) -> tuple[str, str]:
            """Look up the best old topic by keyword Jaccard. Returns
            (label, description), or ('', '') if no good match."""
            if not options["keep_labels"] or not new_keywords:
                return "", ""
            new_set = {k.lower() for k in new_keywords}
            best_score = 0.0
            best = None
            for old_label, old_desc, old_set in carry.values():
                inter = len(new_set & old_set)
                if not inter:
                    continue
                score = inter / len(new_set | old_set)
                if score > best_score:
                    best_score = score
                    best = (old_label, old_desc)
            if best and best_score >= KEEP_LABEL_JACCARD_THRESHOLD:
                return best
            return "", ""

        with transaction.atomic():
            # Topics are fully derived, so wipe + recreate (SET_NULL nulls
            # Evidence.topic; we re-bind below). Keywords are NOT wiped: they
            # carry curator edits (custom_label, enabled) that must survive a
            # refit, so they're reconciled by lemma further down. Clear only the
            # derived M2M links here; they're rebuilt against the kept rows.
            Topic.objects.all().delete()
            Evidence.keywords.through.objects.all().delete()

            # Flat leaves (incl. the outlier cluster) — no hierarchy.
            leaf_pk_by_tid: dict[int, int] = {}
            for tid in sorted(leaf_keywords):
                kws = leaf_keywords[tid]
                if tid == -1:
                    label = "Outliers"
                    carried_desc = ""
                else:
                    carried_label, carried_desc = _carry(kws)
                    label = carried_label or _auto_label(kws)
                leaf = Topic.objects.create(
                    bertopic_id=tid,
                    label=label[:255],
                    description=carried_desc,
                    keywords=kws,
                    size=leaf_sizes[tid],
                    fit_at=now,
                )
                leaf_pk_by_tid[tid] = leaf.pk

            # Keyword rows for the kept lemmas — upserted by lemma so a
            # curator's custom_label / enabled edits survive the refit. The fit
            # owns only the derived fields (label, df, fit_at); the curated
            # fields are left untouched by update_or_create's `defaults`.
            keyword_pk_by_lemma: dict[str, int] = {}
            for lemma in sorted(kept_lemmas):
                kw_obj, _created = Keyword.objects.update_or_create(
                    lemma=lemma,
                    defaults={
                        "label": candidate_surface[lemma][:100],
                        "df": lemma_df[lemma],
                        "fit_at": now,
                    },
                )
                keyword_pk_by_lemma[lemma] = kw_obj.pk

            # Reconcile keywords that fell out of this fit's candidate set:
            # drop the un-curated ones (pure noise), but keep any a curator
            # touched (renamed, disabled, or grouped) so their work isn't lost —
            # just zero their df so they don't rank, since they have no links
            # this round.
            stale = Keyword.objects.exclude(lemma__in=kept_lemmas)
            stale.filter(custom_label="", enabled=True, group__isnull=True).delete()
            stale.update(df=0)

            for ev, raw, tid, (x, y) in zip(
                evidences, raw_topics, topics, coords, strict=True
            ):
                ev.topic_id = leaf_pk_by_tid[tid]
                ev.topic_x = float(x)
                ev.topic_y = float(y)
                ev.topic_fit_at = now
                # Genuine outlier (-1) that reduction moved into a real topic.
                ev.topic_reassigned = raw == -1 and tid != -1

            fields = ["topic", "topic_x", "topic_y", "topic_fit_at", "topic_reassigned"]
            Evidence.objects.bulk_update(evidences, fields, batch_size=500)

            # Evidence ↔ keyword links, built straight into the through table.
            Through = Evidence.keywords.through
            through_rows = [
                Through(evidence_id=ev.pk, keyword_id=keyword_pk_by_lemma[lemma])
                for ev, matched in zip(evidences, ev_lemmas, strict=True)
                for lemma in matched
                if lemma in kept_lemmas
            ]
            Through.objects.bulk_create(through_rows, batch_size=1000)

            if options["reset"]:
                fitted_ids = {e.pk for e in evidences}
                Evidence.objects.exclude(pk__in=fitted_ids).update(
                    topic=None,
                    topic_x=None,
                    topic_y=None,
                    topic_fit_at=None,
                    topic_reassigned=False,
                )

        n_leaves = len([t for t in leaf_keywords if t != -1])
        n_outliers = leaf_sizes.get(-1, 0)
        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Fitted {len(evidences)} pieces of evidence into "
                f"{n_leaves} topics ({n_outliers} outliers); "
                f"{len(through_rows)} evidence-keyword links across "
                f"{len(keyword_pk_by_lemma)} keywords."
            )
        )

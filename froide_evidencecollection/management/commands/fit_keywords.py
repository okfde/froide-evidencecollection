"""Extract per-evidence keywords with KeyBERT and store the evidence ↔ keyword
index plus each evidence's 2D embedding coordinates for the cloud view.

Keywords come straight from KeyBERT, which scores candidate keyphrases against
each document's own embedding and keeps the most salient.

Design "B" — KeyBERT is the *linker*, not just a candidate generator: an
evidence is linked to exactly the keyphrases KeyBERT judged salient *for that
evidence*. A keyword's document frequency is therefore "documents KeyBERT picked
it for", and a facet selection narrows to the evidence KeyBERT associated with
the concept — which may include evidence that implies the concept without
containing the literal word (the looser, semantic matching we wanted to test).

Each evidence's input text is `Evidence.topic_text` — the source text assembled
highest-signal-first, since the embedding model truncates to a token window.

The cloud's dot *positions* come from a 2D UMAP pass over the same embeddings.
The `Evidence.topic` FK and `Topic` rows are intentionally left untouched here;
the view's "is fitted" gate reads `topic_fit_at` instead, which this command
sets.

Run manually (or via cron); the cloud view reads the cached numbers.

Requires: keybert, sentence-transformers, umap-learn, scikit-learn, numpy,
simplemma. Install the bundled extra with `uv pip install -e '.[keywords]'`.
"""

import hashlib
import os
import re
import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from froide_evidencecollection.models import Evidence, Keyword

# Curated German stopword list, expanded with social-media filler. Used to
# filter candidate keyphrases *after* ngram formation: a term is dropped when
# its first or last token is a stopword (so a connective stopword inside a
# longer phrase, "schutz der demokratie", survives). This is deliberately not
# sklearn's stop_words (which strips stopwords before forming ngrams and so
# fuses non-adjacent words into phrases that never occurred). Inlined to avoid
# pulling in nltk/spacy.
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
infolgedessen ins insofern inzwischen irgend irgendwie ist

ja jahr jahre jahren je jede jedem jeden jeder jedermann jedermanns jedoch
jemand jemandem jemanden jene jenem jenen jener jenes jetzt

kam kann kannst kaum kein keine keinem keinen keiner kleine kleinen kleiner
kleines kommen kommt können könnt konnte könnte konnten kurz

lang lange lassen lässt leicht leider letztendlich lieber los

machen macht machte mag magst mal man manche manchem manchen mancher manches mehr
mein meine meinem meinen meiner meines mich mir mit mittel mittlerweile mochte möchte mochten
mögen möglich mögt morgen muss muß müssen musst müsst musste mussten

na nach nachdem nahm nämlich natürlich ne neben nein neue neuen neun neunte neunten neunter
neuntes nicht nichts nie niemand niemandem niemanden noch nun nur

ob oben oder offen oft ohne

quasi

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
""".split()

# A keyword only becomes a facet if KeyBERT picked it for at least this many
# pieces of evidence — drops one-off keyphrases that would clutter the cloud.
DEFAULT_KEYWORD_MIN_DF = 3
# Default embeddings cache: a .npz under the repo-root .cache/ dir (gitignored).
# Resolved from this file's location so it's stable regardless of cwd:
# commands → management → froide_evidencecollection → repo root.
DEFAULT_EMBEDDINGS_CACHE = str(
    Path(__file__).resolve().parents[3] / ".cache" / "fit_keywords_embeddings.npz"
)
# Token pattern for lemmatising keyphrases: word characters incl. German
# umlauts/ß, length >= 2. Lower-cased before matching.
_WORD_RE = re.compile(r"[0-9a-zà-öø-ÿ]{2,}", re.IGNORECASE)


def _make_lemmatizer():
    """Return a cached German lemmatiser ``str -> str`` backed by simplemma.

    A small dict cache keeps repeated tokens cheap across the whole corpus."""
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


def _lemmatize_phrase(phrase: str, lemmatize) -> str:
    """Lemmatise a (possibly multi-word) keyphrase token-by-token and rejoin.

    Per-token lemmatisation is what makes ngram keyphrases normalise correctly:
    simplemma can't lemmatise "soziale medien" as one string, but lemmatising
    "soziale" and "medien" separately and rejoining gives a stable key that a
    surface variant ("sozialen medien") collapses onto."""
    tokens = _WORD_RE.findall(phrase)
    return " ".join(lemmatize(t) for t in tokens if lemmatize(t))


class Command(BaseCommand):
    help = "Extract per-evidence keywords with KeyBERT and store the index + 2D coords."

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
            "--top-n",
            type=int,
            default=10,
            help=(
                "How many keyphrases KeyBERT extracts per piece of evidence "
                "(its top-N by similarity to the document). More = denser "
                "links and more candidate keywords (default: 10)."
            ),
        )
        parser.add_argument(
            "--ngram-max",
            type=int,
            default=2,
            help=(
                "Longest keyphrase length in words: 1 = single words only, "
                "2 = also bigrams, 3 = also trigrams (default: 2)."
            ),
        )
        parser.add_argument(
            "--min-score",
            type=float,
            default=0.0,
            help=(
                "Drop KeyBERT keyphrases whose cosine similarity to the "
                "document is below this, before they ever become links "
                "(0.0 keeps all; try 0.3–0.4 to keep only strong matches; "
                "default: 0.0)."
            ),
        )
        parser.add_argument(
            "--diversity",
            type=float,
            default=None,
            help=(
                "Enable KeyBERT Maximal Marginal Relevance with this diversity "
                "(0 = relevance only, 1 = maximally diverse). Reduces near-"
                "duplicate keyphrases per document. Omit to disable MMR "
                "(default: off)."
            ),
        )
        parser.add_argument(
            "--keyword-min-df",
            type=int,
            default=DEFAULT_KEYWORD_MIN_DF,
            help=(
                "A keyword only becomes a facet if KeyBERT picked it for at "
                "least this many pieces of evidence (default: %(default)s)."
            ),
        )
        parser.add_argument(
            "--vectorizer-min-df",
            type=int,
            default=2,
            help=(
                "Minimum corpus document frequency for a phrase to even be a "
                "KeyBERT *candidate*. The main speed knob: KeyBERT embeds the "
                "whole candidate vocabulary, and with bigrams most phrases "
                "occur once — raising this prunes that long tail before "
                "embedding (1 = no pruning; default: 2)."
            ),
        )
        parser.add_argument(
            "--max-candidates",
            type=int,
            default=None,
            help=(
                "Hard cap on candidate-vocabulary size (CountVectorizer "
                "max_features, keeping the most frequent phrases). Bounds "
                "KeyBERT's embedding cost on large corpora (default: no cap)."
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
            import numpy as np
            from keybert import KeyBERT
            from sentence_transformers import SentenceTransformer
            from sklearn.feature_extraction.text import CountVectorizer
            from umap import UMAP
        except ImportError as exc:
            raise CommandError(
                "Missing dependency: %s. Install with `uv pip install keybert`."
                % exc.name
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
        # touches so iterating doesn't fan out to per-row queries.
        qs = (
            Evidence.objects.all()
            .select_related(
                "social_media_post__account",
                "social_media_post__redistributes__account",
                "social_media_post__redistributes__redistributes__account",
            )
            .prefetch_related(
                "mentions__category",
            )
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

        # Load both embedding caches up front (doc vectors keyed by text hash,
        # candidate-word vectors keyed by the phrase). Encode only what's
        # missing; the file is rewritten once both are filled, below.
        doc_cache, word_cache = self._load_embedding_cache(
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

        # ── KeyBERT extraction ───────────────────────────────────────────
        # Reuse the loaded embedder (no second model) and the precomputed doc
        # embeddings (no second encode). KeyBERT scores candidate keyphrases by
        # cosine similarity to each document, returning the top-N per document.
        #
        # Candidate vocabulary. Crucially we do NOT hand the stopwords to
        # CountVectorizer: sklearn strips stopwords *before* forming ngrams, so
        # non-adjacent words fuse into phrases that never occurred ("schule
        # lerne" out of "… Schule und lerne"). Instead we form ngrams over the
        # raw token stream (true adjacency), then drop a term only when its
        # first or last token is a stopword. That removes unigram stopwords and
        # dangling-stopword bigrams ("die medien", "medien und"), while letting
        # a connective stopword sit *inside* a longer phrase ("schutz der
        # demokratie") survive. `--vectorizer-min-df` prunes the long tail first
        # (the main speed lever, since KeyBERT embeds every candidate);
        # `--max-candidates` optionally caps what's left by frequency.
        prevectorizer = CountVectorizer(
            ngram_range=(1, options["ngram_max"]),
            min_df=options["vectorizer_min_df"],
        )
        counts = prevectorizer.fit_transform(docs)
        all_terms = [str(t) for t in prevectorizer.get_feature_names_out()]
        stop = set(GERMAN_STOPWORDS)

        def _edges_ok(term: str) -> bool:
            toks = term.split()
            return toks[0] not in stop and toks[-1] not in stop

        # Drop any phrase containing a bare-number token (year/count/time): a
        # standalone number is never a useful facet. Filtered here, at the
        # candidate stage, rather than in `_clean_topic_text` so numbers still
        # reach the document embedding (where "50 Prozent", "Artikel 3" carry
        # signal). Phrases are dropped, not fused across the number — same as the
        # stopword edge rule — so number-free unigrams still stand on their own.
        def _no_bare_number(term: str) -> bool:
            return not any(tok.isdigit() for tok in term.split())

        keep = [t for t in all_terms if _edges_ok(t) and _no_bare_number(t)]
        max_candidates = options["max_candidates"]
        if max_candidates and len(keep) > max_candidates:
            # Rank the surviving terms by document frequency, keep the top slice.
            df = np.asarray((counts > 0).sum(axis=0)).ravel()
            df_by_term = dict(zip(all_terms, df.tolist(), strict=True))
            keep.sort(key=lambda t: df_by_term[t], reverse=True)
            keep = keep[:max_candidates]
        if not keep:
            raise CommandError(
                "No candidate keyphrases survived stopword/min-df filtering. "
                "Lower --vectorizer-min-df or import more evidence."
            )
        # Pin the final vectorizer to the filtered vocabulary; KeyBERT restricts
        # each document to the vocabulary terms it actually contains, and
        # re-fitting a fixed vocabulary leaves get_feature_names_out() order
        # stable — so it matches the `words` we embed here.
        vectorizer = CountVectorizer(
            ngram_range=(1, options["ngram_max"]),
            vocabulary=keep,
        )
        # get_feature_names_out yields numpy.str_; cast so cache-key dict lookups
        # (keys reload as plain str) compare identically.
        words = [str(w) for w in vectorizer.get_feature_names_out()]
        t0 = time.perf_counter()
        word_embeddings, n_word_enc = self._encode_with_cache(
            words, words, word_cache, embedder
        )
        self.stdout.write(
            f"Extracting keyphrases with KeyBERT ({len(words)} candidate phrases "
            f"at min_df={options['vectorizer_min_df']}: "
            f"{len(words) - n_word_enc} cached, {n_word_enc} encoded "
            f"in {time.perf_counter() - t0:.1f}s)…"
        )

        # Both caches are now fully populated for this run — persist once.
        self._save_embedding_cache(
            options["embeddings_cache"],
            options["embedding_model"],
            options["max_seq_length"],
            doc_cache,
            word_cache,
        )

        kw_model = KeyBERT(model=embedder)
        extract_kwargs = {
            "vectorizer": vectorizer,
            "top_n": options["top_n"],
            "doc_embeddings": np.asarray(embeddings),
            "word_embeddings": np.asarray(word_embeddings),
        }
        if options["diversity"] is not None:
            extract_kwargs["use_mmr"] = True
            extract_kwargs["diversity"] = options["diversity"]
        kb_t0 = time.perf_counter()
        # Returns one list of (phrase, score) per document, in docs order.
        per_doc = kw_model.extract_keywords(docs, **extract_kwargs)
        kb_elapsed = time.perf_counter() - kb_t0
        self.stdout.write(
            f"  KeyBERT done in {kb_elapsed:.1f}s "
            f"({kb_elapsed / len(docs) * 1000:.1f} ms/doc)."
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

        # ── Keyword index (Design B: KeyBERT picks ARE the links) ─────────
        # For each document, lemmatise its kept keyphrases; that lemma set is
        # the document's links. A lemma's df is the number of documents whose
        # KeyBERT picks included it. Every surface form a lemma appeared in is
        # tallied across documents; the most common form becomes its label.
        self.stdout.write("Building keyword index…")
        lemmatize = _make_lemmatizer()
        min_score = options["min_score"]

        # lemma -> {surface form (lower-cased) -> document count}
        surface_counts: dict[str, dict[str, int]] = {}
        ev_lemmas: list[set[str]] = []
        lemma_df: dict[str, int] = {}
        # Salience aggregates per lemma: the max and the sum/count of the KeyBERT
        # scores of every pick that produced it, for the cached salience fields.
        # A lemma can be produced by several phrases in one document; each such
        # pick contributes its score, so the mean is over picks, not documents.
        lemma_score_max: dict[str, float] = {}
        lemma_score_sum: dict[str, float] = {}
        lemma_score_n: dict[str, int] = {}
        for picks in per_doc:
            matched: set[str] = set()
            for phrase, score in picks:
                if score < min_score:
                    continue
                lemma = _lemmatize_phrase(phrase, lemmatize)
                if not lemma:
                    continue
                matched.add(lemma)
                # Count one occurrence of this surface form for this document.
                # Normalise to lower-case so "Soziale Medien"/"soziale medien"
                # collapse, matching how KeyBERT candidates are cased.
                forms = surface_counts.setdefault(lemma, {})
                surface = phrase.lower()
                forms[surface] = forms.get(surface, 0) + 1
                # Accumulate this pick's salience score.
                if score > lemma_score_max.get(lemma, float("-inf")):
                    lemma_score_max[lemma] = score
                lemma_score_sum[lemma] = lemma_score_sum.get(lemma, 0.0) + score
                lemma_score_n[lemma] = lemma_score_n.get(lemma, 0) + 1
            ev_lemmas.append(matched)
            for lemma in matched:
                lemma_df[lemma] = lemma_df.get(lemma, 0) + 1

        # Sort each lemma's forms most-frequent-first (ties broken by the form
        # for determinism); the first key is the most common form → the label.
        surface_forms: dict[str, dict[str, int]] = {
            lemma: dict(sorted(forms.items(), key=lambda kv: (-kv[1], kv[0])))
            for lemma, forms in surface_counts.items()
        }

        min_df = options["keyword_min_df"]
        kept_lemmas = {lemma for lemma, df in lemma_df.items() if df >= min_df}
        self.stdout.write(
            f"Keyword index: {len(kept_lemmas)} keywords (of "
            f"{len(surface_forms)} candidates) with df >= {min_df}."
        )

        # Salience instrumentation: show how representativeness scores are
        # distributed for kept vs. sub-threshold ("rare") candidates, so a future
        # keep/rescue rule can be set from real numbers rather than guessed.
        lemma_score_mean = {
            lemma: lemma_score_sum[lemma] / lemma_score_n[lemma] for lemma in lemma_df
        }
        self._print_salience_report(
            lemma_df, lemma_score_max, lemma_score_mean, surface_forms, min_df
        )

        now = timezone.now()

        with transaction.atomic():
            # Keywords are NOT wiped: they carry curator edits (custom_label,
            # enabled) that must survive a refit, so they're reconciled
            # by lemma below. Clear only the derived M2M links here.
            Evidence.keywords.through.objects.all().delete()

            # Keyword rows for the kept lemmas — upserted by lemma so a curator's
            # custom_label / enabled edits survive. The fit owns only the
            # derived fields (label, df, fit_at).
            keyword_pk_by_lemma: dict[str, int] = {}
            for lemma in sorted(kept_lemmas):
                forms = surface_forms[lemma]
                # forms is already sorted most-frequent-first, so the first key
                # is the most common surface form → the display label.
                label = next(iter(forms))
                kw_obj, _created = Keyword.objects.update_or_create(
                    lemma=lemma,
                    defaults={
                        "label": label[:100],
                        "surface_forms": forms,
                        "df": lemma_df[lemma],
                        "salience_max": lemma_score_max[lemma],
                        "salience_mean": lemma_score_sum[lemma] / lemma_score_n[lemma],
                        "fit_at": now,
                    },
                )
                keyword_pk_by_lemma[lemma] = kw_obj.pk

            # Reconcile keywords that fell out of this fit: drop the un-curated
            # ones (pure noise), but keep any a curator touched (renamed or
            # disabled) — just zero their derived fields so they don't rank.
            stale = Keyword.objects.exclude(lemma__in=kept_lemmas)
            stale.filter(custom_label="", enabled=True).delete()
            stale.update(df=0, salience_max=0.0, salience_mean=0.0)

            for ev, (x, y) in zip(evidences, coords, strict=True):
                ev.topic_x = float(x)
                ev.topic_y = float(y)
                ev.topic_fit_at = now

            Evidence.objects.bulk_update(
                evidences, ["topic_x", "topic_y", "topic_fit_at"], batch_size=500
            )

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
                    topic_x=None,
                    topic_y=None,
                    topic_fit_at=None,
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"Done. Fitted {len(evidences)} pieces of evidence; "
                f"{len(through_rows)} evidence-keyword links across "
                f"{len(keyword_pk_by_lemma)} keywords."
            )
        )

    def _print_salience_report(
        self, lemma_df, score_max, score_mean, surface_forms, min_df, top_n=15
    ):
        """Print how keyword salience (KeyBERT's max cosine to the document) is
        distributed for kept vs. sub-threshold ("rare") candidates, plus the
        decision-relevant extremes.

        Read-only instrumentation — it changes nothing about what is kept. The
        point is to see whether salience actually separates relevant rare
        keywords from rare noise before wiring it into a keep/rescue rule.
        Remember salience is *representativeness of a document*, not global
        relevance, so a high-salience rare term can still be off-topic."""
        import numpy as np

        def label_of(lemma):
            forms = surface_forms.get(lemma)
            return next(iter(forms)) if forms else lemma

        kept = [lm for lm, df in lemma_df.items() if df >= min_df]
        rare = [lm for lm, df in lemma_df.items() if df < min_df]
        all_max = np.array([score_max[lm] for lm in lemma_df], dtype=float)
        if all_max.size == 0:
            return
        lo, hi = float(all_max.min()), float(all_max.max())

        def summarize(name, lemmas):
            if not lemmas:
                self.stdout.write(f"  {name}: (none)")
                return
            vals = np.array([score_max[lm] for lm in lemmas], dtype=float)
            self.stdout.write(
                f"  {name} (N={len(lemmas)}): "
                f"min={vals.min():.3f} median={np.median(vals):.3f} "
                f"mean={vals.mean():.3f} max={vals.max():.3f}"
            )
            for line in self._histogram_lines(vals, lo, hi, np):
                self.stdout.write("    " + line)

        self.stdout.write("\nSalience (KeyBERT max cosine of a pick to its document):")
        summarize(f"kept (df >= {min_df})", kept)
        summarize(f"rare (df <  {min_df})", rare)

        def dump(title, lemmas):
            self.stdout.write(f"\n  {title}")
            for lm in lemmas:
                self.stdout.write(
                    f"    {label_of(lm)[:30]:30} df={lemma_df[lm]:>3} "
                    f"smax={score_max[lm]:.3f} smean={score_mean[lm]:.3f}"
                )

        # Weakest kept = noise we keep on frequency alone; strongest rare = the
        # relevant-looking terms a df cut would drop (the rescue pool).
        if kept:
            dump(
                "Weakest-salience KEPT keywords (kept on df — noise?):",
                sorted(kept, key=lambda lm: score_max[lm])[:top_n],
            )
        if rare:
            dump(
                "Strongest-salience RARE keywords (dropped by df — rescue?):",
                sorted(rare, key=lambda lm: score_max[lm], reverse=True)[:top_n],
            )
        self.stdout.write("")

    @staticmethod
    def _histogram_lines(vals, lo, hi, np, bins=12, width=40):
        """Text histogram of ``vals`` over ``[lo, hi]`` as a list of bar lines."""
        if hi <= lo:
            return [f"all ≈ {lo:.3f}"]
        edges = np.linspace(lo, hi, bins + 1)
        counts, _ = np.histogram(vals, bins=edges)
        peak = int(counts.max()) or 1
        return [
            f"{edges[k]:.3f}–{edges[k + 1]:.3f} | "
            f"{'#' * int(round(int(counts[k]) / peak * width))} {int(counts[k])}"
            for k in range(bins)
        ]

    def _load_embedding_cache(self, cache_path, model_name, max_seq_length):
        """Load the doc + word embedding caches from ``cache_path``.

        Returns ``(doc_cache, word_cache)``: a ``text-hash -> vector`` dict and
        a ``phrase -> vector`` dict. Both are empty when caching is off, the
        file is absent/unreadable, or it was written with a different embedding
        model / max-seq-length — a vector means nothing under a different
        encoder, so a mismatch silently recomputes everything.
        """
        import numpy as np

        doc_cache: dict[str, "np.ndarray"] = {}
        word_cache: dict[str, "np.ndarray"] = {}
        if not cache_path or not os.path.exists(cache_path):
            return doc_cache, word_cache
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
                return {}, {}
            doc_cache = dict(
                zip(data["doc_keys"].tolist(), data["doc_vectors"], strict=False)
            )
            word_cache = dict(
                zip(data["word_keys"].tolist(), data["word_vectors"], strict=False)
            )
        except Exception as exc:  # corrupt / old-format / unreadable → recompute
            self.stdout.write(f"  embeddings cache unreadable ({exc}); recomputing.")
            return {}, {}
        return doc_cache, word_cache

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

    def _save_embedding_cache(
        self, cache_path, model_name, max_seq_length, doc_cache, word_cache
    ):
        """Persist both caches to a single ``.npz``, tagged with the encoder.

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
            word_keys=np.array(list(word_cache.keys())),
            word_vectors=np.asarray(list(word_cache.values()))
            if word_cache
            else np.empty((0, 0)),
        )

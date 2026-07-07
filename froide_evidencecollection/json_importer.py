import json
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import zip_longest

from django.core.files import File
from django.db import transaction

from froide.georegion.models import GeoRegion
from froide_evidencecollection.models import (
    Actor,
    Chapter,
    Evidence,
    EvidenceMention,
    InstitutionalLevel,
    Organization,
    Person,
    PoliticalPosition,
    Role,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.utils import (
    ImportStatsCollection,
    apply_org_label_replacement,
    equals,
    load_org_label_replacements,
    normalize_name,
    to_dict,
)

logger = logging.getLogger(__name__)


PLATFORM_MAP = {
    "facebook": SocialMediaAccount.Platform.FACEBOOK,
    "instagram": SocialMediaAccount.Platform.INSTAGRAM,
    "telegram": SocialMediaAccount.Platform.TELEGRAM,
    "tiktok": SocialMediaAccount.Platform.TIKTOK,
    "twitter": SocialMediaAccount.Platform.TWITTER,
    "youtube": SocialMediaAccount.Platform.YOUTUBE,
}


# Heuristic mapping from a (typo-cleaned) function label to a canonical,
# gender-neutral role name. Ordered most-specific first; the first pattern that
# matches the lowercased label wins (so "Vorstandsmitglied" beats "Mitglied",
# "Bundessprecher" beats "Sprecher", etc.). This is the pure *parsing* half of
# role handling — it returns a name string and touches no database. Best-effort:
# labels that match nothing get no role, to be filled in during cleanup.
ROLE_RULES = [
    (r"stellvertretende\w*\s+bundessprecher", "Stellvertretende*r Bundessprecher*in"),
    (r"bundessprecher", "Bundessprecher*in"),
    (
        r"stellvertretende\w*\s+bundesvorsitzende",
        "Stellvertretende*r Bundesvorsitzende*r",
    ),
    (r"bundesvorsitzende", "Bundesvorsitzende*r"),
    (
        r"stellvertretende\w*\s+landesvorsitzende",
        "Stellvertretende*r Landesvorsitzende*r",
    ),
    (r"landesvorsitzende", "Landesvorsitzende*r"),
    (r"ehrenvorsitzende", "Ehrenvorsitzende*r"),
    (r"fraktionsvorsitzende", "Fraktionsvorsitzende*r"),
    (r"stellvertretende\w*\s+vorsitzende", "Stellvertretende*r Vorsitzende*r"),
    (r"vorsitzende", "Vorsitzende*r"),
    (r"parlamentarische\w*\s+geschäftsführer", "Parlamentarische*r Geschäftsführer*in"),
    (r"innenpolitische\w*\s+sprecher", "Innenpolitische*r Sprecher*in"),
    (r"stellvertretende\w*\s+sprecher", "Stellvertretende*r Sprecher*in"),
    (r"sprecher", "Sprecher*in"),
    (r"bürgermeister", "Bürgermeister*in"),
    (r"kandidat", "Kandidat*in"),
    # Council seats (kommunale Mandate) are distinct roles, *not* "Abgeordnete*r"
    # (that title is reserved for parliaments). Matched before the generic
    # abgeordnete/mitglied rules below so "Kreistagsabgeordneter" and "Mitglied
    # des Stadtrats" resolve to their seat, not to Abgeordnete*r/Mitglied.
    (r"stadtrat|stadträt", "Stadtrat*rätin"),
    (r"kreisrat|kreisrät|kreistag", "Kreisrat*rätin"),
    (r"gemeinderat|gemeinderät", "Gemeinderat*rätin"),
    (r"bezirksrat|bezirksrät", "Bezirksrat*rätin"),
    # Advisory council bodies (Bezirks-/Stadtbezirksbeirat).
    (r"beirat|beirät", "Beirat*rätin"),
    # Parliamentary mandates: the dump's acronyms (MdB/MdL/MdEP/MdA/MdHB/MEP)
    # alongside the spelled-out form.
    (r"\bmd(b|l|a|ep|hb)\b|\bmep\b|abgeordnete", "Abgeordnete*r"),
    (r"präsident", "Präsident*in"),
    (r"vorstandsmitglied", "Vorstandsmitglied"),
    (r"mitglied", "Mitglied"),
]
ROLE_RULES = [(re.compile(pattern), name) for pattern, name in ROLE_RULES]

# Heuristic mapping from a label to a canonical institutional-level name. Order
# matters: a mandate acronym (MdB/MdL/…) pins the governing level first, so an
# MdB who also chairs a Kreisverband lands on Bund rather than Kreis. Among the
# keyword rules, "Bund" is checked before "Kreis" so a federal candidacy "im
# Landkreis …" lands on Bund, and "Kreis" before "Land" so "Landkreis …" (a
# place qualifier) lands on Kreis rather than Land. The names must match existing
# `InstitutionalLevel` rows — `_resolve_level` only links, never creates — so if
# the level vocabulary differs in the database, adjust the names here.
LEVEL_RULES = [
    (r"\bmdep\b|\bmep\b", "AfD-Europafraktion"),
    (r"\bmdb\b", "AfD-Bundespartei"),
    # MdL (Landtag), MdA (Berlin Abgeordnetenhaus), MdHB (Hamburger Bürgerschaft).
    (r"\bmdl\b|\bmda\b|\bmdhb\b", "AfD-Landesverbände"),
    (r"europ", "AfD-Europafraktion"),
    (r"bund", "AfD-Bundespartei"),
    (
        r"kreis|bezirk|stadt|gemeinde|ortschaft|kommun"
        r"|bürgermeister|bürgerschaft|\bkv\b|\bov\b|\bbv\b",
        "AfD-Kreisverbände",
    ),
    # Below the Kommune rule so a local office (e.g. a Bezirksverband chair) that
    # merely mentions a future Abgeordnetenhaus candidacy keeps its local level;
    # a bare candidacy with no local token still lands on Land here.
    (r"abgeordnetenhaus", "AfD-Landesverbände"),
    (r"land", "AfD-Landesverbände"),
]
LEVEL_RULES = [(re.compile(pattern), name) for pattern, name in LEVEL_RULES]


def parse_role(label):
    """Canonical role name for a function label, or "" if none matches."""
    text = (label or "").lower()
    for pattern, name in ROLE_RULES:
        if pattern.search(text):
            return name
    return ""


def parse_level(label):
    """Canonical institutional-level name for a label, or "" if none matches."""
    text = (label or "").lower()
    for pattern, name in LEVEL_RULES:
        if pattern.search(text):
            return name
    return ""


def _parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


class JSONImporter:
    """
    Import the social-media JSON dump into Evidence + SocialMediaPost rows.

    Expects input that has already been normalized by scripts/prepare_import.py
    (top-level fields named uniformly across platforms, an `account` dict per
    post, and a `references` list for quote/repost relationships).

    Each post becomes one SocialMediaPost and (if not yet curated) one
    Evidence linked via Evidence.social_media_post. Each SocialMediaAccount is
    upserted once per run from its freshest snapshot across all of its posts
    (see `_upsert_accounts`), and its profile is refreshed only when that
    snapshot is at least as new as the account's stored `collected_at`.

    Quote/repost references are materialized inline from each post's
    `references` list (stub accounts/posts via get_or_create). Replies are
    resolved in a second pass over posts inserted during this run. A final
    sweep re-runs the relation seeding so cross-references between Evidence
    pairs created in the same run pick each other up.

    Tracks per-model create/update/delete/skip counts via
    ``ImportStatsCollection`` so the run can be persisted to
    ``ImportExportRun.changes``.
    """

    ACCOUNT_PROFILE_FIELDS = (
        "username",
        "display_name",
        "description",
        "url",
        "follower_count",
    )

    def __init__(self, json_path, dry_run=False):
        self.json_path = json_path
        self.dry_run = dry_run
        self.stats = ImportStatsCollection()
        # (storage, name) pairs written this run, deleted if the run fails.
        self._written_media_files = []
        # (account_id, platform_post_id) -> SocialMediaPost.id
        self._post_index = {}
        # (platform, platform_user_id) -> SocialMediaAccount, upserted once per
        # run from each account's freshest snapshot (see `_upsert_accounts`).
        self._account_index = {}
        # SocialMediaPost.id -> (reply_to_platform_post_id, account_id)
        self._pending_replies = {}
        # verband value (Bundesland name or "Bund") -> GeoRegion or None
        self._region_cache = {}
        # canonical role name -> Role
        self._role_cache = {}
        # canonical institutional-level name -> InstitutionalLevel or None
        self._level_cache = {}
        # Same dump-label corrections align_org_names applies, so an org's
        # abbreviated dump label resolves to its expanded Organization name.
        self._org_label_replacements = load_org_label_replacements()

    def load(self):
        with open(self.json_path) as f:
            return json.load(f)

    @transaction.atomic
    def run(self):
        # Media files are written to storage as rows are created, but storage
        # writes aren't transactional: if the atomic block below rolls back, the
        # rows vanish while the files linger as orphans. Track what we write and
        # delete it on failure. (A successful re-import would overwrite the same
        # deterministic paths anyway, so deleting on failure is always safe.)
        self._written_media_files = []
        try:
            self._run()
        except Exception:
            self._discard_written_media_files()
            raise

    def _run(self):
        data = self.load()
        actor_index, ambiguous = self._build_actor_index()

        # The dump groups posts by scrape target, but one and the same post can
        # be grouped under several targets (e.g. a person and their party's
        # Landesverband), each carrying its *own* report_data — different
        # footnotes/citations for the same post. Since all occurrences share one
        # SocialMediaPost (keyed on account + platform_post_id) and thus one
        # Evidence, importing each occurrence on its own would make the later one
        # delete the earlier one's mentions (see `_upsert_mentions`). So collect
        # occurrences by post identity first, merging their report_data and
        # accumulating the set of targets they were grouped under, then import
        # each post once with the union of all its mentions and originators.
        merged_items = {}
        order = []
        # Freshest account snapshot per account, collected while walking the
        # dump so each account row is written exactly once (see
        # `_record_account_snapshot` / `_upsert_accounts`).
        account_snapshots = {}
        # account key -> set of raw `account_label`s seen for it. Unlike the
        # scrape-target grouping, `account_label` names the actor that actually
        # owns the posting account, so `_upsert_accounts` resolves it to set
        # `SocialMediaAccount.actor`.
        account_labels = defaultdict(set)
        for entry in data.values():
            target = self._resolve_target(entry, actor_index, ambiguous)
            if target is None:
                continue
            # Ensure the Actor exists: it is referenced by functions and, below,
            # recorded as an originator of every post grouped under this target.
            # The account that posted a given post is linked separately, to the
            # owner its `account_label` names rather than to this scrape target
            # (see `_upsert_accounts`); the grouping only attests authorship.
            actor = self._get_or_create_actor(target)
            self._set_verband(target, entry)
            if isinstance(target, Person):
                self._import_functions(target, entry)
            for platform, items in (entry.get("social_media") or {}).items():
                if platform not in PLATFORM_MAP:
                    logger.warning("Unknown platform %r; skipping", platform)
                    continue

                for item in items:
                    key = self._post_identity(platform, item)
                    self._record_account_snapshot(account_snapshots, platform, item)
                    account_key = self._account_key(platform, item.get("account"))
                    for label in (item.get("report_data") or {}).get(
                        "account_label"
                    ) or []:
                        account_labels[account_key].add(label)
                    # One originator id per report_data row, so a merged post's
                    # mentions can be attributed to whoever they were grouped
                    # under (see `_upsert_mentions`). The rows of one occurrence
                    # all belong to this target; row count is the (equal) length
                    # of its report_data lists.
                    row_count = self._report_row_count(item.get("report_data") or {})
                    existing = merged_items.get(key)
                    if existing is None:
                        merged_items[key] = {
                            "platform": platform,
                            "item": item,
                            "originator_ids": {actor.id},
                            "mention_originators": [actor.id] * row_count,
                        }
                        order.append(key)
                    else:
                        self._merge_report_data(existing["item"], item)
                        existing["originator_ids"].add(actor.id)
                        existing["mention_originators"].extend([actor.id] * row_count)

        self._upsert_accounts(account_snapshots, account_labels, actor_index, ambiguous)

        for key in order:
            merged = merged_items[key]
            self._import_item(
                merged["platform"],
                merged["item"],
                merged["originator_ids"],
                merged["mention_originators"],
            )

        self._resolve_replies()

    @staticmethod
    def _post_identity(platform, item):
        """Identity tuple matching the (account, platform_post_id) post key.

        Mirrors how `_upsert_account`/`_upsert_post` identify a row, so two dump
        occurrences that would map to the same SocialMediaPost share one key.
        """
        account = item.get("account") or {}
        return (
            platform,
            str(account.get("platform_user_id")),
            str(item.get("platform_post_id")),
        )

    @staticmethod
    def _account_key(platform, account_data):
        """Identity of the account a post belongs to, matching `_upsert_account`."""
        return (platform, str((account_data or {}).get("platform_user_id")))

    @staticmethod
    def _is_newer(candidate, current):
        """True if ``candidate`` collected_at is strictly newer than ``current``.

        A missing collected_at (``None``) carries no freshness signal and ranks
        oldest, so a dateless snapshot never displaces a dated one and ties keep
        the incumbent.
        """
        if candidate is None:
            return False
        return current is None or candidate > current

    def _record_account_snapshot(self, snapshots, platform, item):
        """Keep the freshest snapshot of the account that posted ``item``.

        Each post carries its own snapshot of its account (``item["account"]``),
        and the same account recurs across many posts with differing follower
        counts, names and ``collected_at``. Upserting the row once per post would
        let those disagreeing snapshots overwrite each other — flapping in the
        stats and leaving whichever post happened to be processed last as the
        winner. Instead keep only the newest snapshot per account here and write
        it once in `_upsert_accounts`.
        """
        account_data = item.get("account") or {}
        key = self._account_key(platform, account_data)
        collected_at = _parse_dt(item.get("collected_at"))
        current = snapshots.get(key)
        if current is None or self._is_newer(collected_at, current["collected_at"]):
            snapshots[key] = {"data": account_data, "collected_at": collected_at}

    def _upsert_accounts(self, snapshots, account_labels, actor_index, ambiguous):
        """Upsert each account once from its freshest snapshot, indexing the rows.

        `_import_item` then looks the account up by `_account_key` instead of
        re-upserting it per post. Each account is also linked to the actor its
        `account_label` names (see `_resolve_account_owner`).
        """
        self._account_index = {}
        for key, snap in snapshots.items():
            platform, _puid = key
            owner = self._resolve_account_owner(
                snap["data"], account_labels.get(key, set()), actor_index, ambiguous
            )
            self._account_index[key] = self._upsert_account(
                platform, snap["data"], snap["collected_at"], owner
            )

    def _resolve_account_owner(self, account_data, labels, actor_index, ambiguous):
        """Resolve the actor that owns an account from its `account_label`(s).

        The dump groups posts by scrape target, but `account_label` names the
        account's actual owner — often a different actor (e.g. a party
        Landesverband's account surfacing under a person's grouping). Resolved
        through the same actor index as the scrape targets, so the org-label
        expansions, spelling fixes and aliases stay in lockstep.

        Returns the owner Actor, or ``None`` (leaving the account unlinked) when
        no label is given, the labels disagree, or the named actor is ambiguous
        or absent — warning on every case where a label was present but could
        not be resolved.
        """
        # Collapse to distinct owners; an account should name exactly one. Apply
        # the org-label expansion first so e.g. "BV Lichtenberg" resolves like it
        # does for scrape targets.
        distinct = {}
        for raw in labels:
            key = normalize_name(
                apply_org_label_replacement(raw, self._org_label_replacements)
            )
            if key:
                distinct.setdefault(key, raw)
        if not distinct:
            return None  # external account or reference-only stub; leave unlinked

        username = account_data.get("username") or account_data.get("platform_user_id")
        if len(distinct) > 1:
            msg = (
                f"Account {username!r} has conflicting account_labels "
                f"{sorted(distinct.values())!r}; leaving owner unset"
            )
            logger.warning(msg)
            self.stats.track_skipped(SocialMediaAccount, msg)
            return None

        key, raw = next(iter(distinct.items()))
        if key in ambiguous:
            msg = (
                f"account_label {raw!r} (account {username!r}) matches multiple "
                "actors; leaving owner unset"
            )
            logger.warning(msg)
            self.stats.track_skipped(SocialMediaAccount, msg)
            return None

        target = actor_index.get(key)
        if target is None:
            # Fallback: a dump `account_label` may carry middle names the actor's
            # stored name omits ("Thorsten Paul Moriße" vs "Thorsten Moriße").
            # Retry on first + last name only — but solely after the full name
            # missed, so genuine multi-part names ("Armin Paul Hampel") still
            # match as given and don't collapse onto a different person.
            short = normalize_name(self._drop_middle_names(raw))
            if short != key and short not in ambiguous:
                target = actor_index.get(short)
        if target is None:
            msg = (
                f"No actor found for account_label {raw!r} (account {username!r}); "
                "leaving owner unset"
            )
            logger.warning(msg)
            self.stats.track_skipped(SocialMediaAccount, msg)
            return None

        return self._get_or_create_actor(target)

    @staticmethod
    def _drop_middle_names(label):
        """First + last whitespace-token of a (comma-swapped) personal name, e.g.
        "Thorsten Paul Moriße" -> "Thorsten Moriße". Names with two or fewer
        tokens are returned unchanged."""
        tokens = label.split()
        if len(tokens) > 2:
            return f"{tokens[0]} {tokens[-1]}"
        return label

    @staticmethod
    def _merge_report_data(base, extra):
        """Fold another occurrence's report_data into ``base`` in place.

        The report_data values are row-parallel lists (``topic[i]`` belongs with
        ``footnote_id[i]`` etc.), so concatenating them per key keeps each
        occurrence's rows aligned while unioning the mentions across occurrences.
        Duplicate rows are harmless: `_upsert_mentions` collapses them on their
        footnote.
        """
        base_rd = base.get("report_data") or {}
        extra_rd = extra.get("report_data") or {}
        merged = dict(base_rd)
        for field in set(base_rd) | set(extra_rd):
            merged[field] = (base_rd.get(field) or []) + (extra_rd.get(field) or [])
        base["report_data"] = merged

    @staticmethod
    def _report_row_count(report_data):
        """Number of mention rows in a single occurrence's report_data.

        The row-parallel lists `_upsert_mentions` consumes are equal-length
        within one occurrence (validated against the dump), so the count is the
        longest of them — robust to a field being absent on a given post.
        """
        fields = ("topic", "footnote_id", "fliesstext")
        return max((len(report_data.get(f) or []) for f in fields), default=0)

    def _discard_written_media_files(self):
        # Best-effort removal of files written during a run that then failed; a
        # delete error is logged but never masks the original import exception.
        for storage, name in self._written_media_files:
            try:
                storage.delete(name)
            except OSError:
                logger.warning("Could not delete orphaned media file %r", name)
        self._written_media_files = []

    def log_stats(self):
        """Return collected stats in the standard ``ImportExportRun.changes`` shape."""
        return self.stats.to_dict()

    @classmethod
    def _account_profile_values(cls, account_data):
        """Profile fields present in ``account_data``, mapped to model fields.

        Shared by the main-post upsert and the stub creation so a referenced
        account is stored with everything the reference carries (telegram only
        has the ID; twitter references carry the full profile).
        """
        values = {
            field: account_data[field]
            for field in cls.ACCOUNT_PROFILE_FIELDS
            if account_data.get(field) is not None
        }
        is_verified = account_data.get("is_verified")
        is_blue_verified = account_data.get("is_blue_verified")
        if is_verified is not None or is_blue_verified is not None:
            values["is_verified"] = bool(is_verified) or bool(is_blue_verified)
        return values

    def _build_actor_index(self):
        """Map normalized name -> Person/Organization for resolving dump labels.

        Persons are keyed on ``"first last"`` (the form the dump's ``label``
        uses) and organizations on their name; both also register every
        ``also_known_as`` alias, so renamed orgs still match. A name that would
        resolve to more than one distinct target is recorded as ambiguous and
        skipped at lookup time rather than guessed.
        """
        index = {}
        ambiguous = set()

        def register(target, names):
            for name in names:
                key = normalize_name(name)
                if not key:
                    continue
                existing = index.get(key)
                if existing is not None and existing != target:
                    ambiguous.add(key)
                index[key] = target

        for person in Person.objects.all():
            register(
                person,
                [f"{person.first_name} {person.last_name}", *person.also_known_as],
            )
        for org in Organization.objects.all():
            register(org, [org.organization_name, *org.also_known_as])

        return index, ambiguous

    def _resolve_target(self, entry, actor_index, ambiguous):
        """Resolve a dump entry to its Person/Organization, or None to skip."""
        label = entry.get("label")
        if label and entry.get("ent_type") == "o":
            # Match the expanded name align_org_names stored (e.g. the dump's
            # "BV Lichtenberg" resolves to "Bezirksverband Lichtenberg").
            label = apply_org_label_replacement(label, self._org_label_replacements)
        key = normalize_name(label) if label else ""
        if not key:
            msg = f"Entry without usable label; skipping (label={label!r})"
            logger.warning(msg)
            self.stats.track_skipped(Actor, msg)
            return None
        if key in ambiguous:
            msg = f"Label {label!r} matches multiple actors; skipping"
            logger.warning(msg)
            self.stats.track_skipped(Actor, msg)
            return None
        target = actor_index.get(key)
        if target is None:
            msg = f"No actor found for label {label!r}; skipping"
            logger.warning(msg)
            self.stats.track_skipped(Actor, msg)
            return None
        return target

    def _get_or_create_actor(self, target):
        try:
            return target.actor
        except Actor.DoesNotExist:
            self.stats.reset_instance(Actor)
            field = "person" if isinstance(target, Person) else "organization"
            actor = Actor.objects.create(**{field: target})
            self.stats.track_created(Actor, actor)
            return actor

    def _set_verband(self, target, entry):
        # Set the actor's `verband` (Person/Organization) from the dump entry.
        # Never overwrite a resolved value with None: an unresolved verband
        # (typo, blank) shouldn't wipe a value already on the actor.
        region = self._resolve_region(entry.get("verband"))
        if region is None or target.verband_id == region.id or self.dry_run:
            return
        old_data = to_dict(target)
        target.verband = region
        self.stats.reset_instance(type(target))
        target.save()
        self.stats.track_updated(type(target), old_data, target)

    # ------------------------------------------------------------------
    # Political positions (per-person "functions" list)
    # ------------------------------------------------------------------
    def _import_functions(self, person, entry):
        # The dump lists a person's functions as free-text strings, most relevant
        # first; we keep only the first as that person's single PoliticalPosition.
        # Idempotent: matched on label and only updated when an import-owned field
        # changed. Add/update-only (never deletes), so a curator's own positions
        # and manual fixes survive re-runs.
        functions = entry.get("functions") or []
        if not functions or self.dry_run:
            return

        label = (functions[0] or "").strip()
        if not label:
            return

        # Heuristic, curator-correctable links: set on create and only filled in
        # later if still empty, so a re-import never clobbers a manual fix.
        role = self._resolve_role(label)
        level = self._resolve_level(label)

        existing = {p.label: p for p in person.political_positions.all()}
        position = existing.get(label)
        if position is None:
            self.stats.reset_instance(PoliticalPosition)
            position = PoliticalPosition.objects.create(
                person=person,
                label=label,
                role=role,
                institutional_level=level,
            )
            self.stats.track_created(PoliticalPosition, position)
            return

        old_data = to_dict(position)
        update = False
        # Override policy: only populate the heuristic / matched links while
        # empty, so a re-import never clobbers a manual fix.
        if role is not None and position.role_id is None:
            position.role = role
            update = True
        if level is not None and position.institutional_level_id is None:
            position.institutional_level = level
            update = True
        if update:
            self.stats.reset_instance(PoliticalPosition)
            position.save()
            self.stats.track_updated(PoliticalPosition, old_data, position)

    def _resolve_role(self, label):
        # Parse a canonical role name from the (clean) label and link it to a Role
        # row, creating the row on first sight. Cached per run. Returns None when
        # the label matches no rule.
        name = parse_role(label)
        if not name:
            return None
        if name not in self._role_cache:
            role, created = Role.objects.get_or_create(name=name)
            if created:
                self.stats.reset_instance(Role)
                self.stats.track_created(Role, role)
            self._role_cache[name] = role
        return self._role_cache[name]

    def _resolve_level(self, label):
        # Parse a canonical institutional-level name from the label and look up
        # the matching InstitutionalLevel. Lookup-only (never creates): an
        # unmatched name resolves to None and is logged, since the level
        # vocabulary is curated elsewhere. Cached per run.
        name = parse_level(label)
        if not name:
            return None
        if name not in self._level_cache:
            level = InstitutionalLevel.objects.filter(name=name).first()
            if level is None:
                logger.warning("No InstitutionalLevel %r for label %r", name, label)
            self._level_cache[name] = level
        return self._level_cache[name]

    def _resolve_region(self, name):
        # Resolve a `verband` value to its GeoRegion. A Bundesland name maps to
        # the matching state region; the special value "Bund" (the federal level)
        # maps to the country-level region. Cached per run; unmatched names
        # (e.g. typos) resolve to None and are logged — fixing them is part of
        # the separate cleanup step.
        name = (name or "").strip()
        if not name:
            return None
        if name not in self._region_cache:
            if name == "Bund":
                region = GeoRegion.objects.filter(kind="country").first()
            else:
                region = GeoRegion.objects.filter(name=name, kind="state").first()
            if region is None:
                logger.warning("No GeoRegion found for verband %r", name)
            self._region_cache[name] = region
        return self._region_cache[name]

    # ------------------------------------------------------------------
    # Per-item import
    # ------------------------------------------------------------------
    def _import_item(self, platform, item, originator_ids=(), mention_originators=()):
        account_data = item["account"]
        platform_post_id = str(item["platform_post_id"])
        posted_at = _parse_dt(item.get("created_at"))
        edited_at = _parse_dt(item.get("edited_at"))
        collected_at = _parse_dt(item.get("collected_at"))

        # The account row was already upserted once per run from its freshest
        # snapshot (see `_upsert_accounts`); reuse it rather than writing again.
        account = self._account_index[self._account_key(platform, account_data)]

        if self.dry_run:
            return

        references = item.get("references") or []
        # `image_alt_text` accompanies `image_file`: a dict carrying the
        # curator's alt text. Media is now tracked by source path on the post
        # itself (only the screenshot is stored as a file, attached below).
        alt = item.get("image_alt_text") or {}
        post_fields = {
            "url": item["url"],
            "posted_at": posted_at,
            "edited_at": edited_at,
            "text": item.get("text") or "",
            "title": item.get("title") or "",
            "description": item.get("description") or "",
            "transcription": item.get("transcription") or "",
            "image_source_path": item.get("image_file") or "",
            "image_description": (alt.get("alt_text") or "").strip(),
            "video_source_path": item.get("video_file") or "",
            "screenshot_source_path": item.get("screenshot_file") or "",
            "view_count": item.get("view_count"),
            "like_count": item.get("like_count"),
            "comment_count": item.get("comment_count"),
            "is_comment_disabled": item.get("is_comment_disabled"),
            "share_count": item.get("share_count"),
            "reactions": item.get("reactions"),
            "user_snapshot": account_data,
            # A reference without a platform_post_id can't become a stub post
            # (see _upsert_stub_post); keep it verbatim so the fact that this
            # post redistributes *something* survives. A post redistributes at
            # most one post, so this is a single object, not a list.
            "unresolved_redistribution": next(
                (ref for ref in references if not ref.get("platform_post_id")),
                None,
            ),
        }
        evidence_fields = {
            "documentation_date": collected_at.date() if collected_at else None,
        }

        post = self._upsert_post(account, platform_post_id, post_fields)
        self._attach_screenshot(post, item)
        evidence = self._upsert_evidence(post, evidence_fields)
        self._seed_originators_from_targets(evidence, originator_ids)
        self._upsert_mentions(evidence, item, mention_originators)

        self._post_index[(account.id, post.platform_post_id)] = post.id

        # Redistributed post — repost/quote/forward (inline stub creation).
        # `redistributes` is a single FK, but a tweet can carry both a quote and
        # a retweet reference; link only one so the slot isn't written twice
        # (which flapped in the stats and left the loser as an orphan stub).
        ref = self._select_redistribution(references)
        if ref is not None:
            stub_post = self._upsert_stub_post(platform, ref)
            if stub_post:
                self._link_reference(post, stub_post, ref)

        # Replies are resolved in a second pass (target may not yet exist).
        reply_id = self._extract_reply_id(platform, item)
        if reply_id is not None:
            self._pending_replies[post.id] = (str(reply_id), account.id)

    def _seed_originators_from_targets(self, evidence, originator_ids):
        """Record the scrape targets a post was grouped under as originators.

        The dump groups each post under the actor(s) it documents; one post can
        sit under several (e.g. two speakers in one video, or a person and their
        party association). Those actors — and *only* those — are the evidence's
        originators: who holds the posting account is never assumed to be the
        originator (the account is never even linked to an actor by the import,
        see `_upsert_account`). Add-only and idempotent (M2M `add` ignores
        members already present), so curator edits survive re-runs.
        """
        if originator_ids:
            evidence.originators.add(*originator_ids)

    def _upsert_post(self, account, platform_post_id, post_fields):
        self.stats.reset_instance(SocialMediaPost)
        post = SocialMediaPost.objects.filter(
            account=account, platform_post_id=platform_post_id
        ).first()
        if post is None:
            post = SocialMediaPost.objects.create(
                account=account,
                platform_post_id=platform_post_id,
                **post_fields,
            )
            self.stats.track_created(SocialMediaPost, post)
            return post

        old_data = to_dict(post)
        update = False
        for field, value in post_fields.items():
            if not equals(getattr(post, field), value):
                setattr(post, field, value)
                update = True
        if update:
            post.save()
            self.stats.track_updated(SocialMediaPost, old_data, post)
        return post

    # ------------------------------------------------------------------
    # Post screenshot (the only file-backed post media)
    # ------------------------------------------------------------------
    def _attach_screenshot(self, post, item):
        # Copy the archival screenshot bytes onto the post. Image/video content
        # files are no longer stored — only tracked by source path on the post
        # (set in `post_fields`). The `screenshot_source_path` is likewise set in
        # `post_fields`; here we attach the file, backfilling a post that has no
        # screenshot file yet. Counted under the post's own create/update, so no
        # separate stats are emitted.
        source_path = item.get("screenshot_file")
        if not source_path or post.screenshot:
            return
        self._attach_media_file(post, source_path, "screenshot")
        if post.screenshot:
            post.save(update_fields=["screenshot"])

    @staticmethod
    def _parse_timestamp(value):
        # "HH:MM:SS" (or "MM:SS") -> timedelta; blank/garbage -> None. Excerpt
        # times are best-effort metadata, so an unparseable value is logged and
        # stored as None rather than failing the import.
        if not value:
            return None
        try:
            nums = [int(p) for p in value.split(":")]
        except (ValueError, AttributeError):
            logger.warning("Unparseable video timestamp %r; storing None.", value)
            return None
        if len(nums) == 3:
            hours, minutes, seconds = nums
        elif len(nums) == 2:
            hours, minutes, seconds = 0, nums[0], nums[1]
        else:
            logger.warning("Unexpected video timestamp %r; storing None.", value)
            return None
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)

    def _attach_media_file(self, media, source_path, field_name="screenshot"):
        # Import bundles ship media as paths relative to the JSON file
        # (e.g. "./screenshot/foo.png"); resolve against its directory and copy
        # the bytes into the named FieldFile (the post's `screenshot`). A missing
        # file is tolerated — the post still carries its text/source paths — but
        # logged so the miss is visible.
        if not source_path:
            return
        base_dir = os.path.dirname(os.path.abspath(self.json_path))
        abs_path = os.path.normpath(os.path.join(base_dir, source_path))
        if not os.path.isfile(abs_path):
            # Don't fail the import, but make the miss visible: it lands in
            # ImportExportRun.changes so a silently-empty file is traceable.
            msg = f"Media file not found, saved row without file: {abs_path}"
            logger.warning(msg)
            self.stats.track_skipped(type(media), msg)
            return
        field = getattr(media, field_name)
        with open(abs_path, "rb") as fh:
            field.save(os.path.basename(abs_path), File(fh), save=False)
        # Remember the written file so a later import failure can clean it up
        # (storage writes aren't covered by the surrounding transaction).
        self._written_media_files.append((field.storage, field.name))

    def _upsert_evidence(self, post, evidence_fields):
        self.stats.reset_instance(Evidence)
        evidence = Evidence.objects.filter(social_media_post=post).first()
        if evidence is None:
            evidence = Evidence.objects.create(
                social_media_post=post,
                **evidence_fields,
            )
            self.stats.track_created(Evidence, evidence)
            return evidence

        old_data = to_dict(evidence)
        update = False
        for field, value in evidence_fields.items():
            if not equals(getattr(evidence, field), value):
                setattr(evidence, field, value)
                update = True
        if update:
            evidence.save()
            self.stats.track_updated(Evidence, old_data, evidence)
        return evidence

    @staticmethod
    def _select_redistribution(references):
        """The single reference to link via `redistributes`, or None.

        Only references carrying a ``platform_post_id`` can become a linkable
        stub (see `_upsert_stub_post`); among those the last one wins, preserving
        the target the former link-every-reference loop ended on for tweets that
        both quote and retweet. Id-less references are handled separately via the
        post's ``unresolved_redistribution`` field.
        """
        linkable = [ref for ref in references if ref.get("platform_post_id")]
        return linkable[-1] if linkable else None

    def _link_reference(self, post, stub_post, ref):
        self.stats.reset_instance(SocialMediaPost)
        if equals(post.redistributes_id, stub_post.id):
            return
        old_data = to_dict(post)
        post.redistributes = stub_post
        post.save(update_fields=["redistributes"])
        self.stats.track_updated(SocialMediaPost, old_data, post)

    @staticmethod
    def _extract_reply_id(platform, item):
        if platform == "telegram":
            reply_to = item.get("reply_to") or {}
            return reply_to.get("reply_to_msg_id")
        if platform == "twitter":
            return item.get("in_reply_to_status_id")
        return None

    # ------------------------------------------------------------------
    # Stub account/post upsert (referenced but not directly scraped)
    # ------------------------------------------------------------------
    def _upsert_stub_post(self, platform, ref):
        platform_post_id = ref.get("platform_post_id")
        if not platform_post_id:
            # Telegram hidden-forward origin: only a display name/timestamp,
            # no stable post id (and sometimes no account id). Without a
            # platform_post_id we can't create a uniquely-identifiable stub —
            # the (account, platform_post_id) constraint would collapse all
            # id-less posts into one row — so we skip the redistributes link.
            return None

        self.stats.reset_instance(SocialMediaAccount)
        self.stats.reset_instance(SocialMediaPost)
        platform_value = PLATFORM_MAP[platform]
        acct_data = ref["account"]
        platform_user_id = str(acct_data["platform_user_id"])

        account, created_account = SocialMediaAccount.objects.get_or_create(
            platform=platform_value,
            platform_user_id=platform_user_id,
            defaults={
                "actor": None,
                **self._account_profile_values(acct_data),
            },
        )
        if created_account:
            self.stats.track_created(SocialMediaAccount, account)

        post, created_post = SocialMediaPost.objects.get_or_create(
            account=account,
            platform_post_id=str(platform_post_id),
            defaults={
                "url": ref.get("url") or "",
                "posted_at": _parse_dt(ref.get("created_at")),
                "text": ref.get("text") or "",
            },
        )
        if created_post:
            self.stats.track_created(SocialMediaPost, post)
            self._post_index[(account.id, post.platform_post_id)] = post.id
        return post

    # ------------------------------------------------------------------
    # Account upsert + profile freshness
    # ------------------------------------------------------------------
    def _upsert_account(self, platform, account_data, collected_at, owner=None):
        self.stats.reset_instance(SocialMediaAccount)
        platform_value = PLATFORM_MAP[platform]
        platform_user_id = str(account_data["platform_user_id"])
        username = account_data.get("username") or ""
        account = SocialMediaAccount.objects.filter(
            platform=platform_value, platform_user_id=platform_user_id
        ).first()
        created = account is None

        if created:
            account = SocialMediaAccount(
                platform=platform_value,
                platform_user_id=platform_user_id,
                username=username,
            )
            old_data = {}
        else:
            old_data = to_dict(account)

        update = False

        # Link the account to the owner its `account_label` named
        # (`_resolve_account_owner`). Add-only, like the profile fields: an
        # unresolved owner (None) never wipes a link already on the row.
        if owner is not None and not equals(account.actor_id, owner.id):
            account.actor = owner
            update = True

        # Refresh the profile (and advance collected_at) only when this snapshot
        # is at least as fresh as what we stored. A dateless snapshot
        # (collected_at None, e.g. telegram/youtube) carries no freshness
        # signal: it may fill in a still-dateless account — a stub seen for the
        # first time, or a brand-new row — but must never overwrite, let alone
        # blank out, values a dated import already wrote.
        stored = account.collected_at
        if collected_at is None:
            should_refresh = stored is None
        else:
            should_refresh = stored is None or collected_at >= stored
        if should_refresh:
            for field, value in self._account_profile_values(account_data).items():
                if not equals(getattr(account, field), value):
                    setattr(account, field, value)
                    update = True

            if not equals(account.collected_at, collected_at):
                account.collected_at = collected_at
                update = True

        if self.dry_run:
            return account

        if created:
            account.save()
            self.stats.track_created(SocialMediaAccount, account)
        elif update:
            account.save()
            self.stats.track_updated(SocialMediaAccount, old_data, account)

        return account

    # ------------------------------------------------------------------
    # Evidence mentions (chapter/footnote tuples)
    # ------------------------------------------------------------------
    def _upsert_mentions(self, evidence, item, mention_originators=()):
        report_data = item.get("report_data") or {}
        # `topic` is row-parallel to the lists below: index i describes the same
        # mention. Each entry is a root-to-leaf path of theme labels, e.g.
        # ["Menschenwürde", "…", "Ethnisch-kulturelles Volksverständnis der AfD"].
        # The leaf names the specific topic the evidence is filed under, and the
        # whole path materialises the chapter tree (see `_get_or_create_chapter`).
        # The dump also carries the report's physical chapter path in
        # `capitel_structur`, but we deliberately ignore it: it bottoms out at a
        # per-evidence actor+date leaf (1146 distinct paths vs. 30 for `topic`)
        # and is littered with editorial prefixes and organisational nodes, so it
        # would build a chapter node per piece of evidence rather than a clean
        # thematic tree.
        topics = report_data.get("topic") or []
        footnotes = report_data.get("footnote_id") or []
        citations = report_data.get("fliesstext") or []
        # `video_timestamp` is row-parallel to the lists above (validated against
        # the dump): index i carries `{start, end}` for the same mention whose
        # curated quote is `fliesstext[i]`. Present (non-empty) only for video
        # evidence; absent entries leave start/end null.
        video_timestamps = report_data.get("video_timestamp") or []
        # `report_urls` is row-parallel too (added in prepare_import, parallel to
        # `capitel_structur`): index i is the public report-page URL for the same
        # mention. Absent entries leave `report_url` blank.
        report_urls = report_data.get("report_urls") or []
        # `mention_originators` is row-parallel too: the actor id of whoever this
        # row was grouped under, so a merged post's mentions are attributed to
        # the right originator (see `_run`). Empty for callers that pass none.
        if not (topics or footnotes):
            return

        existing = {m.footnote: m for m in evidence.mentions.all()}
        wanted = set()

        for (
            topic_path,
            footnote,
            citation,
            vts,
            report_url,
            originator_id,
        ) in zip_longest(
            topics,
            footnotes,
            citations,
            video_timestamps,
            report_urls,
            mention_originators,
            fillvalue=None,
        ):
            topic_path = self._clean_topic_path(topic_path)
            if not topic_path:
                continue
            footnote = (footnote or "").strip()
            if not footnote:
                raise ValueError(
                    f"Missing footnote for a mention of {evidence} under topic "
                    f"{topic_path!r}; report_data rows are misaligned."
                )
            key = footnote
            wanted.add(key)
            chapter = self._get_or_create_chapter(topic_path)
            vts = vts or {}
            scalar_fields = {
                "chapter_structure": topic_path,
                "citation": citation or "",
                "report_url": report_url or "",
                "start": self._parse_timestamp(vts.get("start")),
                "end": self._parse_timestamp(vts.get("end")),
                "originator_id": originator_id,
            }
            if key in existing:
                mention = existing[key]
                old_data = to_dict(mention)
                changed = False
                if (chapter.id if chapter else None) != mention.chapter_id:
                    mention.chapter = chapter
                    changed = True
                for field, value in scalar_fields.items():
                    if not equals(getattr(mention, field), value):
                        setattr(mention, field, value)
                        changed = True
                if changed:
                    self.stats.reset_instance(EvidenceMention)
                    mention.save()
                    self.stats.track_updated(EvidenceMention, old_data, mention)
                continue
            self.stats.reset_instance(EvidenceMention)
            mention = EvidenceMention.objects.create(
                evidence=evidence,
                footnote=footnote,
                chapter=chapter,
                **scalar_fields,
            )
            self.stats.track_created(EvidenceMention, mention)

        for key, mention in existing.items():
            if key in wanted:
                continue
            self.stats.reset_instance(EvidenceMention)
            mention_id = mention.id
            mention.delete()
            self.stats.track_deleted(EvidenceMention, mention_id)

    @staticmethod
    def _clean_topic_path(raw):
        """Normalise a raw ``topic`` entry into a root-to-leaf label path.

        Accepts the dump's list-of-labels form (and tolerates a bare string for
        older single-label callers), trims blanks, and collapses runs of equal
        adjacent labels — the dump occasionally repeats the leaf (e.g.
        ``["…", "Ausbürgerung", "Ausbürgerung"]``), which would otherwise create a
        chapter node whose only child carries the same label.
        """
        if not raw:
            return []
        if isinstance(raw, str):
            raw = [raw]
        labels = []
        for label in raw:
            label = (label or "").strip()
            if not label or (labels and labels[-1] == label):
                continue
            labels.append(label)
        return labels

    def _get_or_create_chapter(self, labels):
        """Materialise the chapter path and flag its leaf as the main topic.

        ``labels`` is a cleaned root-to-leaf topic path. Builds (or reuses) the
        tree and marks the leaf ``is_main_topic`` — with topic-derived paths the
        leaf is always the specific theme the evidence is filed under. Returns the
        leaf chapter, or ``None`` when there is nothing to build or on a dry run.
        """
        labels = [(label or "").strip() for label in labels]
        labels = [label for label in labels if label]
        if not labels or self.dry_run:
            return None

        leaf = Chapter.get_or_create_from_path(labels)
        if leaf and not leaf.is_main_topic:
            leaf.is_main_topic = True
            leaf.save(update_fields=["is_main_topic"])
        return leaf

    # ------------------------------------------------------------------
    # Second-pass reply resolution
    # ------------------------------------------------------------------
    def _resolve_replies(self):
        if self.dry_run or not self._pending_replies:
            return
        for post_id, (reply_id, account_id) in self._pending_replies.items():
            target = self._post_index.get((account_id, reply_id))
            if target is None:
                continue
            self.stats.reset_instance(SocialMediaPost)
            post = SocialMediaPost.objects.get(pk=post_id)
            if post.reply_to_id == target:
                continue
            old_data = to_dict(post)
            post.reply_to_id = target
            post.save(update_fields=["reply_to"])
            self.stats.track_updated(SocialMediaPost, old_data, post)

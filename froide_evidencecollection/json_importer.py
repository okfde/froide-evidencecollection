import json
import logging
import os
import re
from calendar import monthrange
from datetime import date, datetime
from itertools import zip_longest

from django.core.files import File
from django.db import transaction
from django.db.models import Max

from froide.georegion.models import GeoRegion
from froide_evidencecollection.models import (
    Actor,
    Category,
    Chapter,
    Evidence,
    InstitutionalLevel,
    Organization,
    Person,
    PoliticalPosition,
    PostImage,
    PostScreenshot,
    PostVideo,
    Quote,
    Reference,
    Role,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.relation_seeding import seed_relations_from_source
from froide_evidencecollection.utils import (
    ImportStatsCollection,
    equals,
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


# Maps the dump's "Partei/Parlament" discriminator onto PoliticalPosition.Type.
FUNCTION_TYPE_MAP = {
    "Mandat": PoliticalPosition.Type.MANDATE,
    "Parlament": PoliticalPosition.Type.PARLIAMENT,
    "Partei": PoliticalPosition.Type.PARTY,
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
    (r"abgeordnete", "Abgeordnete*r"),
    (r"präsident", "Präsident*in"),
    (r"vorstandsmitglied", "Vorstandsmitglied"),
    (r"mitglied", "Mitglied"),
    (r"stadtrat|stadträt", "Stadtrat*rätin"),
]
ROLE_RULES = [(re.compile(pattern), name) for pattern, name in ROLE_RULES]

# Heuristic mapping from a label to a canonical institutional-level name. Order
# matters: "Bund" is checked before "Kreis" so a federal candidacy "im
# Landkreis …" lands on Bund, and "Kreis" before "Land" so "Landkreis …" (a
# place qualifier) lands on Kreis rather than Land. The names must match existing
# `InstitutionalLevel` rows — `_resolve_level` only links, never creates — so if
# the level vocabulary differs in the database, adjust the names here.
LEVEL_RULES = [
    (r"europ", "AfD-Europafraktion"),
    (r"bund", "AfD-Bundespartei"),
    (r"kreis|bezirk|stadt|gemeinde|ortschaft|kommun", "AfD-Kreisverbände"),
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


# Extracts the party sub-organization ("Verband") a party-function label refers
# to, so it can be matched against an existing Organization. The label names the
# *board* ("Landesvorstand …") or the *association* ("Landesverband …"); both
# denote the same Organization, which is named as the Verband — so "vorstand" is
# normalised to "verband". A required level prefix (landes/kreis/…) anchors the
# match on the real body and skips a bare leading "des Vorstands …". The trailing
# place ("Thüringen", "Konstanz") is kept. Returns "" when nothing matches.
_ORG_BODY_RE = re.compile(
    r"(landes|bundes|kreis|bezirks|stadt|orts|regional)"
    r"(?:verband|vorstand)(?:e?s)?\b\s*(.*)$"
)


def parse_organization_name(label):
    """Candidate Verband name for a party-function label, or "" if none."""
    match = _ORG_BODY_RE.search((label or "").lower())
    if not match:
        return ""
    prefix, tail = match.group(1), match.group(2).strip()
    name = f"{prefix}verband"
    return f"{name} {tail}" if tail else name


def _parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


def _parse_month(value, end=False):
    """Parse a month-precision "YYYY-MM" string to a date.

    The day is the first of the month for a start date and the last for an end
    date, so the stored DateField round-trips to the same month. Returns None for
    blank or unparseable input (data cleanup is handled in a separate step).
    """
    if not value:
        return None
    try:
        year, month = (int(part) for part in value.split("-"))
    except (ValueError, AttributeError):
        logger.warning("Unparseable month %r; storing None.", value)
        return None
    day = monthrange(year, month)[1] if end else 1
    return date(year, month, day)


def _parse_ja_nein(value):
    # German yes/no flag ("JA"/"NEIN") -> bool; anything else (blank, missing,
    # unexpected) -> None so "unknown" stays distinct from an explicit "no".
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if normalized == "JA":
        return True
    if normalized == "NEIN":
        return False
    return None


class JSONImporter:
    """
    Import the social-media JSON dump into Evidence + SocialMediaPost rows.

    Expects input that has already been normalized by scripts/prepare_import.py
    (top-level fields named uniformly across platforms, an `account` dict per
    post, and a `references` list for quote/repost relationships).

    Each post becomes one SocialMediaPost and (if not yet curated) one
    Evidence linked via Evidence.social_media_post. Profile fields on
    SocialMediaAccount are updated only when the post's `collected_at` is
    newer than the account's `collected_at`.

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
        # (account_id, platform_post_id) -> SocialMediaPost.id
        self._post_index = {}
        # SocialMediaPost.id -> (reply_to_platform_post_id, account_id)
        self._pending_replies = {}
        # verband (Bundesland) name -> GeoRegion or None
        self._region_cache = {}
        # canonical role name -> Role
        self._role_cache = {}
        # canonical institutional-level name -> InstitutionalLevel or None
        self._level_cache = {}
        # normalized Verband candidate -> Organization or None
        self._org_match_cache = {}
        # normalized name -> Person/Organization, plus the ambiguous-name set;
        # populated in run() and reused to match functions to existing orgs.
        self._actor_index = {}
        self._ambiguous_names = set()

    def load(self):
        with open(self.json_path) as f:
            return json.load(f)

    @transaction.atomic
    def run(self):
        data = self.load()
        actor_index, ambiguous = self._build_actor_index()
        # Reused by `_resolve_organization` to link functions to existing orgs.
        self._actor_index = actor_index
        self._ambiguous_names = ambiguous
        # `Evidence.external_id` is globally unique; continue past whatever is
        # already stored so re-runs that create new Evidence don't collide
        # with rows created by an earlier run.
        max_external_id = Evidence.objects.aggregate(m=Max("external_id"))["m"] or 0
        external_id = max_external_id + 1

        for entry in data.values():
            target = self._resolve_target(entry, actor_index, ambiguous)
            if target is None:
                continue
            actor = self._get_or_create_actor(target)
            if isinstance(target, Person):
                self._import_functions(target, entry)
            for platform, items in (entry.get("social_media") or {}).items():
                if platform not in PLATFORM_MAP:
                    logger.warning("Unknown platform %r; skipping", platform)
                    continue

                for item in items:
                    self._import_item(actor, platform, item, external_id)
                    external_id += 1

        self._resolve_replies()

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

    # ------------------------------------------------------------------
    # Political positions (per-person "functions" list)
    # ------------------------------------------------------------------
    def _import_functions(self, person, entry):
        # Map each entry of the person's `functions` list to a PoliticalPosition.
        # Idempotent: existing rows are matched on (type, label, start_date) and
        # only updated when an import-owned field changed. The start date is part
        # of the key (not the end date) because a person can hold the same
        # position in two separate terms — distinguished only by when it started
        # — while the end date floats: ongoing positions carry the current month
        # as their end, so keying on it would spawn a duplicate on every re-run.
        # `organization` is linked to an *existing* Verband only (never created).
        functions = entry.get("functions") or []
        if not functions or self.dry_run:
            return

        existing = {
            (p.type, p.label, p.start_date): p for p in person.political_positions.all()
        }

        for func in functions:
            type_ = FUNCTION_TYPE_MAP.get(func.get("Partei/Parlament"))
            if type_ is None:
                msg = (
                    f"Unknown function type {func.get('Partei/Parlament')!r} for "
                    f"{person}; skipping"
                )
                logger.warning(msg)
                self.stats.track_skipped(PoliticalPosition, msg)
                continue

            label = (func.get("Funktion") or "").strip()
            start_date = _parse_month(func.get("start_datum"))
            quelle = func.get("quelle") or []
            # Source-authoritative scalar fields: refreshed from the dump on every
            # run (type/label/start_date form the match key, so they're equal by
            # construction).
            source_fields = {
                "end_date": _parse_month(func.get("end_datum"), end=True),
                "start_source_url": quelle[0] if len(quelle) > 0 else "",
                "end_source_url": quelle[1] if len(quelle) > 1 else "",
            }
            region = self._resolve_region(func.get("verband"))
            # Heuristic, curator-correctable links: set on create and only filled
            # in later if still empty, so a re-import never clobbers a manual fix.
            role = self._resolve_role(label)
            level = self._resolve_level(label)
            # Only party functions name a Verband; a mandate's body is a
            # parliament, not an Organization.
            organization = (
                self._resolve_organization(label)
                if type_ == PoliticalPosition.Type.PARTY
                else None
            )

            key = (type_, label, start_date)
            position = existing.get(key)
            if position is None:
                self.stats.reset_instance(PoliticalPosition)
                position = PoliticalPosition.objects.create(
                    person=person,
                    type=type_,
                    label=label,
                    start_date=start_date,
                    region=region,
                    role=role,
                    institutional_level=level,
                    organization=organization,
                    **source_fields,
                )
                self.stats.track_created(PoliticalPosition, position)
                existing[key] = position
                continue

            old_data = to_dict(position)
            update = False
            for field, value in source_fields.items():
                if not equals(getattr(position, field), value):
                    setattr(position, field, value)
                    update = True
            # Never overwrite a resolved region with None (an unresolved verband
            # shouldn't wipe a value a curator already set).
            if region is not None and position.region_id != region.id:
                position.region = region
                update = True
            # Override policy: only populate the heuristic / matched links while
            # empty, so a re-import never clobbers a manual fix.
            if role is not None and position.role_id is None:
                position.role = role
                update = True
            if level is not None and position.institutional_level_id is None:
                position.institutional_level = level
                update = True
            if organization is not None and position.organization_id is None:
                position.organization = organization
                update = True
            if update:
                self.stats.reset_instance(PoliticalPosition)
                position.save()
                self.stats.track_updated(PoliticalPosition, old_data, position)

    def _resolve_organization(self, label):
        # Match the label's Verband to an *existing* Organization via the actor
        # index (names + aliases), never creating one. An unmatched or ambiguous
        # name resolves to None (logged) so no junk orgs are introduced. Cached
        # per run. Relies on organizations already being imported (e.g. from
        # NocoDB) before this run.
        candidate = parse_organization_name(label)
        if not candidate:
            return None
        key = normalize_name(candidate)
        if not key:
            return None
        if key not in self._org_match_cache:
            org = None
            if key not in self._ambiguous_names:
                target = self._actor_index.get(key)
                if isinstance(target, Organization):
                    org = target
            if org is None:
                logger.warning(
                    "No organization match for label %r (tried %r)", label, candidate
                )
            self._org_match_cache[key] = org
        return self._org_match_cache[key]

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
        # Resolve a `verband` Bundesland name to its GeoRegion (kind "state").
        # Cached per run; unmatched names (e.g. typos) resolve to None and are
        # logged — fixing them is part of the separate cleanup step.
        name = (name or "").strip()
        if not name:
            return None
        if name not in self._region_cache:
            region = GeoRegion.objects.filter(name=name, kind="state").first()
            if region is None:
                logger.warning("No state GeoRegion found for verband %r", name)
            self._region_cache[name] = region
        return self._region_cache[name]

    # ------------------------------------------------------------------
    # Per-item import
    # ------------------------------------------------------------------
    def _import_item(self, actor, platform, item, external_id):
        account_data = item["account"]
        platform_post_id = str(item["platform_post_id"])
        posted_at = _parse_dt(item.get("created_at"))
        edited_at = _parse_dt(item.get("edited_at"))
        collected_at = _parse_dt(item.get("collected_at"))

        account = self._upsert_account(actor, platform, account_data, collected_at)

        if self.dry_run:
            return

        references = item.get("references") or []
        post_fields = {
            "url": item["url"],
            "posted_at": posted_at,
            "edited_at": edited_at,
            "text": item.get("text") or "",
            "title": item.get("title") or "",
            "view_count": item.get("view_count"),
            "like_count": item.get("like_count"),
            "comment_count": item.get("comment_count"),
            "is_comment_disabled": item.get("is_comment_disabled"),
            "share_count": item.get("share_count"),
            "reactions": item.get("reactions"),
            "user_snapshot": account_data,
            "raw": item,
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
        self._import_media(post, item)
        evidence = self._upsert_evidence(post, external_id, evidence_fields)
        # References (and their quotes) first, so seeding has quotes to attach
        # the originator to — originators live on the quote, not the evidence.
        self._upsert_references(evidence, item)
        seed_relations_from_source(evidence)

        self._post_index[(account.id, post.platform_post_id)] = post.id

        # Redistributed posts — repost/quote/forward (inline stub creation).
        for ref in references:
            stub_post = self._upsert_stub_post(platform, ref)
            if not stub_post:
                continue
            self._link_reference(post, stub_post, ref)

        # Replies are resolved in a second pass (target may not yet exist).
        reply_id = self._extract_reply_id(platform, item)
        if reply_id is not None:
            self._pending_replies[post.id] = (str(reply_id), account.id)

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
    # Post media (images / videos)
    # ------------------------------------------------------------------
    def _import_media(self, post, item):
        # An image, (at most one) video, and an archival screenshot of the post.
        # An image's content_text is curator-filled; a video carries its full
        # `transcription` text (searched) plus the report's relevant time ranges
        # (`video_timestamp`, parked verbatim); a screenshot is a pure provenance
        # file. The per-media text feeds Evidence.text_segments. Each single-file
        # field is a path string (or None when absent). `image_alt_text`
        # accompanies `image_file`: a dict carrying the curator's alt text
        # (`alt_text`) and whether the image relates to the post's text
        # (`text_bezug_zum_bild`, "JA"/"NEIN").
        image_path = item.get("image_file")
        if image_path:
            alt = item.get("image_alt_text") or {}
            self._upsert_file_media(
                post,
                PostImage,
                image_path,
                extra_fields={
                    "description": (alt.get("alt_text") or "").strip(),
                    "is_related_to_text": _parse_ja_nein(
                        alt.get("text_bezug_zum_bild")
                    ),
                },
            )

        # The `transcription` text, `srt_file` sidecar and `video_timestamp`
        # ranges only accompany a video, so the video exists only when there's a
        # video file. The video file itself is not stored — only its path
        # (`source_path`) for reference.
        video_path = item.get("video_file")
        if video_path:
            report_data = item.get("report_data") or {}
            self._upsert_video(
                post,
                video_path,
                transcript_text=(item.get("transcription") or "").strip(),
                timestamps=report_data.get("video_timestamp") or [],
                transcript_path=item.get("srt_file") or "",
            )

        screenshot_path = item.get("screenshot_file")
        if screenshot_path:
            self._upsert_file_media(post, PostScreenshot, screenshot_path)

    def _upsert_file_media(self, post, model, source_path, extra_fields=None):
        # Shared upsert for file-backed media (PostImage, PostScreenshot): create
        # the row, or backfill the file on a row that predates file storage or
        # whose file could not be resolved on an earlier run. `extra_fields`
        # carries import-owned scalar fields (an image's `description` /
        # `is_related_to_text`) that are set on create and overwritten on
        # re-import; a screenshot passes none. An image's on-screen
        # `content_text` is curator-filled (never imported), so it is untouched.
        extra_fields = extra_fields or {}
        self.stats.reset_instance(model)
        obj = model.objects.filter(post=post, source_path=source_path).first()
        if obj is None:
            obj = model(post=post, source_path=source_path, **extra_fields)
            self._attach_media_file(obj, source_path)
            obj.save()
            self.stats.track_created(model, obj)
            return obj

        old_data = to_dict(obj)
        changed = False
        if source_path and not obj.file:
            self._attach_media_file(obj, source_path)
            changed = changed or bool(obj.file)
        for field, value in extra_fields.items():
            if not equals(getattr(obj, field), value):
                setattr(obj, field, value)
                changed = True
        if changed:
            obj.save()
            self.stats.track_updated(model, old_data, obj)
        return obj

    def _upsert_video(
        self, post, source_path, transcript_text, timestamps, transcript_path=""
    ):
        # The video file is not stored: `source_path` is kept only as a reference
        # to the original file name. `transcript` and `timestamps` are
        # import-owned and overwritten on re-import (the curator's
        # `transcript_override` is never touched); the SRT sidecar is backfilled
        # if missing.
        self.stats.reset_instance(PostVideo)
        video = PostVideo.objects.filter(post=post, source_path=source_path).first()
        if video is None:
            video = PostVideo(
                post=post,
                source_path=source_path,
                transcript=transcript_text,
                timestamps=timestamps,
            )
            self._attach_media_file(video, transcript_path, "transcript_file")
            video.save()
            self.stats.track_created(PostVideo, video)
            return video

        old_data = to_dict(video)
        update = False
        for field, value in (
            ("transcript", transcript_text),
            ("timestamps", timestamps),
        ):
            if not equals(getattr(video, field), value):
                setattr(video, field, value)
                update = True
        if transcript_path and not video.transcript_file:
            self._attach_media_file(video, transcript_path, "transcript_file")
            update = update or bool(video.transcript_file)
        if update:
            video.save()
            self.stats.track_updated(PostVideo, old_data, video)
        return video

    def _attach_media_file(self, media, source_path, field_name="file"):
        # Import bundles ship media as paths relative to the JSON file
        # (e.g. "./image/foo.jpg"); resolve against its directory and copy the
        # bytes into the named FileField (`file` for an image/screenshot,
        # `transcript_file` for a video's SRT sidecar). A missing file is
        # tolerated — the row still feeds text_segments — but logged so the miss
        # is visible.
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

    def _upsert_evidence(self, post, external_id, evidence_fields):
        self.stats.reset_instance(Evidence)
        evidence = Evidence.objects.filter(social_media_post=post).first()
        if evidence is None:
            evidence = Evidence.objects.create(
                external_id=external_id,
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
                "raw": ref,
            },
        )
        if created_post:
            self.stats.track_created(SocialMediaPost, post)
            self._post_index[(account.id, post.platform_post_id)] = post.id
        return post

    # ------------------------------------------------------------------
    # Account upsert + profile freshness
    # ------------------------------------------------------------------
    def _upsert_account(self, actor, platform, account_data, collected_at):
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
                actor=actor,
            )
            old_data = {}
        else:
            old_data = to_dict(account)

        update = False

        # An account first seen via a reference is created as an orphan stub
        # (actor=None). Adopt it the first time it shows up as a real post.
        if not created and actor is not None:
            if account.actor_id is None:
                account.actor = actor
                update = True
            elif account.actor_id != actor.id:
                logger.warning(
                    "Account %s/%s already linked to actor #%s, not #%s",
                    platform,
                    platform_user_id,
                    account.actor_id,
                    actor.id,
                )

        # Profile fields are written on first sight (including stub adoption,
        # and platforms like telegram/youtube that carry no collected_at) and
        # otherwise refreshed unless this dump is strictly older than what we
        # already stored.
        should_refresh = not (
            collected_at is not None
            and account.collected_at is not None
            and collected_at < account.collected_at
        )
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
    # Quotes (cited spans) and references (category/footnote tuples)
    # ------------------------------------------------------------------
    def _upsert_references(self, evidence, item):
        # The report cites this evidence at one or more footnotes. Each footnote
        # row carries a category, chapter path and a cited text (`fliesstext`).
        # Distinct cited texts become `Quote`s (deduped); each footnote row
        # becomes a `Reference` pointing at the quote for its text. Several
        # references can share one quote (same bit cited under different
        # categories/footnotes); a blank citation means "no sub-selection" and
        # maps to the evidence's single full-source quote.
        report_data = item.get("report_data") or {}
        topics = report_data.get("topic") or []
        footnotes = report_data.get("footnote_id") or []
        chapter_structures = report_data.get("chapter_sturcrue") or []
        citations = report_data.get("fliesstext") or []

        # Normalise the parallel arrays into per-reference rows, dropping rows
        # without a category (the reference's required anchor).
        rows = []
        if topics or footnotes or chapter_structures:
            for category_name, footnote, chapter_structure, citation in zip_longest(
                topics, footnotes, chapter_structures, citations, fillvalue=None
            ):
                category_name = (category_name or "").strip()
                if not category_name:
                    continue
                rows.append(
                    (
                        category_name,
                        (footnote or "").strip(),
                        chapter_structure or [],
                        citation,
                    )
                )

        # With no usable rows, the evidence still has its whole-post claim: a
        # single full-source quote (the cloud point + the originator's home).
        # Leave any quotes from a previous run untouched (parity with the old
        # "no report_data -> no change" behaviour).
        if not rows:
            if not evidence.quotes.exists():
                self.stats.reset_instance(Quote)
                quote = Quote.objects.create(
                    evidence=evidence, text="", is_full_source=True
                )
                self.stats.track_created(Quote, quote)
            return

        # 1. Resolve the quotes these rows need, deduped by normalized cited text.
        quote_by_key = self._sync_quotes(evidence, [citation for *_, citation in rows])

        # 2. Upsert the references, each pointing at its row's quote. Keyed by
        #    (category, footnote) like the old mentions.
        existing = {
            (r.category_id, r.footnote): r
            for r in Reference.objects.filter(quote__evidence=evidence)
        }
        wanted = set()
        for category_name, footnote, chapter_structure, citation in rows:
            category, _ = Category.objects.get_or_create(name=category_name)
            key = (category.id, footnote)
            wanted.add(key)
            chapter = self._get_or_create_chapter(
                chapter_structure, topic=category_name
            )
            quote = quote_by_key[self._quote_key(citation)]
            if key in existing:
                ref = existing[key]
                update_fields = []
                if ref.quote_id != quote.id:
                    ref.quote = quote
                    update_fields.append("quote")
                if chapter is not None and ref.chapter_id != chapter.id:
                    ref.chapter = chapter
                    update_fields.append("chapter")
                if not equals(ref.chapter_structure, chapter_structure):
                    ref.chapter_structure = chapter_structure
                    update_fields.append("chapter_structure")
                if update_fields:
                    ref.save(update_fields=update_fields)
                continue
            self.stats.reset_instance(Reference)
            ref = Reference.objects.create(
                quote=quote,
                category=category,
                footnote=footnote,
                chapter_structure=chapter_structure,
                chapter=chapter,
            )
            self.stats.track_created(Reference, ref)

        for key, ref in existing.items():
            if key in wanted:
                continue
            self.stats.reset_instance(Reference)
            ref_id = ref.id
            ref.delete()
            self.stats.track_deleted(Reference, ref_id)

        # 3. Drop quotes left without any reference (e.g. a citation that no
        #    longer appears in the report).
        self._prune_orphan_quotes(evidence)

    @staticmethod
    def _quote_key(citation):
        # Dedup key for a quote within an evidence. A blank citation means "no
        # sub-selection": it maps to the single full-source quote (the whole
        # post is the relevant text), keyed by "". Otherwise the
        # whitespace-normalized cited text, so citations differing only in
        # whitespace collapse onto one quote (best-effort; cleaned variants that
        # differ in wording stay distinct, which is acceptable).
        text = (citation or "").strip()
        if not text:
            return ""
        return re.sub(r"\s+", " ", text)

    def _sync_quotes(self, evidence, citations):
        # Build {quote_key: Quote} for the distinct cited texts in `citations`.
        # Existing quotes are matched by their imported `text` (full-source by
        # flag), so a curator's `text_override` survives re-import. Returns the
        # mapping; orphan pruning happens in the caller after references are set.
        wanted = {}  # key -> representative imported text ("" for full source)
        for citation in citations:
            key = self._quote_key(citation)
            wanted.setdefault(key, "" if key == "" else (citation or "").strip())

        existing = {}
        for quote in evidence.quotes.all():
            existing["" if quote.is_full_source else self._quote_key(quote.text)] = (
                quote
            )

        result = {}
        for key, text in wanted.items():
            quote = existing.get(key)
            if quote is None:
                self.stats.reset_instance(Quote)
                quote = Quote.objects.create(
                    evidence=evidence,
                    text=text,
                    is_full_source=(key == ""),
                )
                self.stats.track_created(Quote, quote)
            result[key] = quote
        return result

    def _prune_orphan_quotes(self, evidence):
        for quote in evidence.quotes.all():
            if quote.references.exists():
                continue
            self.stats.reset_instance(Quote)
            quote_id = quote.id
            quote.delete()
            self.stats.track_deleted(Quote, quote_id)

    def _get_or_create_chapter(self, labels, topic):
        """Materialise the chapter path and flag the topic node.

        Builds (or reuses) the tree path described by ``labels`` and marks the
        node whose label equals ``topic`` as ``is_main_topic``. Returns the leaf
        chapter, or ``None`` when there is nothing to build or on a dry run.
        """
        labels = [(label or "").strip() for label in labels]
        labels = [label for label in labels if label]
        if not labels or self.dry_run:
            return None

        leaf = Chapter.get_or_create_from_path(labels)
        topic = (topic or "").strip()
        if topic:
            path_nodes = list(leaf.get_ancestors()) + [leaf]
            for node in path_nodes:
                if node.custom_label == topic and not node.is_main_topic:
                    node.is_main_topic = True
                    node.save(update_fields=["is_main_topic"])
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

"""
Seed an evidence's originators from observation-layer source data.

Maps the originating actor recorded on an evidence's source (a
SocialMediaPost) onto the evidence's quotes' `originators` relation —
originators live on the claim (`Quote`), not the evidence, so the post's
author is seeded as the default originator of each of the evidence's quotes
(a curator can override per quote, e.g. for a multi-speaker video).

Idempotent: re-running adds nothing already present (M2M `add` is a no-op
for existing members), so curator edits are preserved across re-runs.
"""

from froide_evidencecollection.models import (
    Actor,
    Evidence,
    SocialMediaPost,
)


def seed_relations_from_source(evidence: Evidence) -> None:
    if evidence.social_media_post_id:
        _seed_from_social_media_post(evidence, evidence.social_media_post)


def _seed_from_social_media_post(evidence: Evidence, post: SocialMediaPost) -> None:
    # Originator — from the account's linked Actor (stub accounts have no actor).
    if post.account_id:
        actor = post.account.actor
        if actor is not None:
            _ensure_originator(evidence, actor)


def _ensure_originator(evidence: Evidence, actor: Actor) -> None:
    for quote in evidence.quotes.all():
        quote.originators.add(actor)

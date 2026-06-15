"""
Seed an evidence's originators from observation-layer source data.

Maps the originating actor recorded on an evidence's source (a
SocialMediaPost) onto the evidence's `originators` relation.

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
    evidence.originators.add(actor)

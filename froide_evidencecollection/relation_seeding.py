"""
Seed curated evidence-level actor relations from observation-layer source data.

Maps the originating actor recorded on an evidence's source (a
SocialMediaPost or Document) onto an EvidenceActorRelation row on the
evidence itself.

Idempotent: re-running adds nothing already present.
Additive: never modifies or deletes existing rows, so curator edits
are preserved across re-runs.
"""

from functools import lru_cache

from froide_evidencecollection.models import (
    Actor,
    Document,
    Evidence,
    EvidenceActorRelation,
    EvidenceActorRelationRole,
    SocialMediaPost,
)


@lru_cache(maxsize=None)
def _actor_role(name: str) -> EvidenceActorRelationRole:
    return EvidenceActorRelationRole.objects.get(name=name)


def seed_relations_from_source(evidence: Evidence) -> None:
    if evidence.social_media_post_id:
        _seed_from_social_media_post(evidence, evidence.social_media_post)
    if evidence.document_id:
        _seed_from_document(evidence, evidence.document)


def _seed_from_social_media_post(evidence: Evidence, post: SocialMediaPost) -> None:
    # posted_by — from the account's linked Actor (stub accounts have no actor).
    if post.account_id:
        actor = post.account.actor
        if actor is not None:
            _ensure_actor_relation(evidence, actor, "posted_by")


def _seed_from_document(evidence: Evidence, document: Document) -> None:
    if document.issuer_id:
        _ensure_actor_relation(evidence, document.issuer, "posted_by")


def _ensure_actor_relation(evidence: Evidence, actor: Actor, role_name: str) -> None:
    EvidenceActorRelation.objects.get_or_create(
        evidence=evidence,
        actor=actor,
        role=_actor_role(role_name),
    )

"""
Seed curated evidence-level relations from observation-layer source data.

Maps relations recorded on an evidence's source (a SocialMediaPost or
Document) onto EvidenceActorRelation / EvidenceRelation rows on the
evidence itself.

Idempotent: re-running adds nothing already present.
Additive: never modifies or deletes existing rows, so curator edits
are preserved across re-runs.

Trade-off: if a curator deletes an auto-seeded relation deliberately,
the next seeding run recreates it. The recommended workaround is to
re-label the relation's role rather than delete the row.
"""

from functools import lru_cache

from froide_evidencecollection.models import (
    Actor,
    Document,
    Evidence,
    EvidenceActorRelation,
    EvidenceActorRelationRole,
    EvidenceRelation,
    EvidenceRelationRole,
    SocialMediaPost,
)


@lru_cache(maxsize=None)
def _actor_role(name: str) -> EvidenceActorRelationRole:
    return EvidenceActorRelationRole.objects.get(name=name)


@lru_cache(maxsize=None)
def _evidence_role(name: str) -> EvidenceRelationRole:
    return EvidenceRelationRole.objects.get(name=name)


# SocialMediaPost.ReferenceType -> EvidenceRelationRole slug
_REFERENCE_TYPE_TO_ROLE = {
    SocialMediaPost.ReferenceType.QUOTE: "quotes",
    SocialMediaPost.ReferenceType.REPOST: "reposts",
}


def seed_relations_from_source(evidence: Evidence) -> None:
    source = evidence.source
    if source is None:
        return
    if isinstance(source, SocialMediaPost):
        _seed_from_social_media_post(evidence, source)
    elif isinstance(source, Document):
        _seed_from_document(evidence, source)


def _seed_from_social_media_post(evidence: Evidence, post: SocialMediaPost) -> None:
    # posted_by — from the account's linked Actor (stub accounts have no actor).
    if post.account_id:
        actor = post.account.actor
        if actor is not None:
            _ensure_actor_relation(evidence, actor, "posted_by")

    # quotes / reposts — typed self-reference on the post.
    if post.references_id and post.reference_type:
        role_name = _REFERENCE_TYPE_TO_ROLE.get(post.reference_type)
        target = _evidence_for_post(post.references_id)
        if role_name and target is not None:
            _ensure_evidence_relation(evidence, target, role_name)

    # replies_to — thread parent on the post.
    if post.reply_to_id:
        target = _evidence_for_post(post.reply_to_id)
        if target is not None:
            _ensure_evidence_relation(evidence, target, "replies_to")


def _seed_from_document(evidence: Evidence, document: Document) -> None:
    if document.issuer_id:
        _ensure_actor_relation(evidence, document.issuer, "posted_by")


def _ensure_actor_relation(evidence: Evidence, actor: Actor, role_name: str) -> None:
    EvidenceActorRelation.objects.get_or_create(
        evidence=evidence,
        actor=actor,
        role=_actor_role(role_name),
    )


def _ensure_evidence_relation(
    evidence: Evidence, target: Evidence, role_name: str
) -> None:
    if evidence.pk == target.pk:
        return  # CheckConstraint forbids self-relation; skip silently.
    EvidenceRelation.objects.get_or_create(
        from_evidence=evidence,
        to_evidence=target,
        role=_evidence_role(role_name),
    )


def _evidence_for_post(post_id: int) -> Evidence | None:
    """Return the Evidence linked to this post via OneToOne, or None.

    Uses the reverse `evidence` relation provided by the OneToOneField on
    Evidence.social_media_post.
    """
    return Evidence.objects.filter(social_media_post_id=post_id).first()

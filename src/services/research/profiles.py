"""Versioned A-share company-profile resolution."""

from __future__ import annotations

from typing import Any, Mapping

from .factor_policy_v1 import PROFILE_RESOLVER_VERSION
from .schemas import ProfileResolution


COMP_TYPE_PROFILES = {
    "1": "industrial",
    "2": "bank",
    "3": "insurance",
    "4": "securities",
}

# Ordered, versioned fallbacks.  Broad labels such as "financial" are omitted
# because they cannot safely distinguish banks, insurers, and brokerages.
PROFILE_KEYWORDS = {
    "bank": ("银行", "bank"),
    "insurance": ("保险", "insurance", "insurer"),
    "securities": ("证券", "券商", "期货", "securities", "brokerage", "broker"),
}


def _normalized_text(value: Any) -> str:
    return str(value or "").strip().casefold()


def _fallback_profile(text: str) -> str | None:
    for profile in ("bank", "insurance", "securities"):
        if any(keyword in text for keyword in PROFILE_KEYWORDS[profile]):
            return profile
    return None


def resolve_company_profile(company: Mapping[str, Any]) -> ProfileResolution:
    """Resolve profile using authoritative ``comp_type`` before fallbacks."""

    raw_comp_type = company.get("comp_type")
    if isinstance(raw_comp_type, Mapping):
        raw_comp_type = raw_comp_type.get("value")
    if isinstance(raw_comp_type, float) and raw_comp_type.is_integer():
        raw_comp_type = int(raw_comp_type)
    comp_type = _normalized_text(raw_comp_type)
    if comp_type in COMP_TYPE_PROFILES:
        return ProfileResolution(
            profile=COMP_TYPE_PROFILES[comp_type],
            source=f"comp_type:{comp_type}",
            resolver_version=PROFILE_RESOLVER_VERSION,
        )

    industry = _normalized_text(company.get("industry"))
    industry_profile = _fallback_profile(industry)
    if industry_profile:
        return ProfileResolution(
            profile=industry_profile,
            source="industry_fallback",
            resolver_version=PROFILE_RESOLVER_VERSION,
        )

    name = _normalized_text(company.get("name") or company.get("stock_name"))
    name_profile = _fallback_profile(name)
    if name_profile:
        return ProfileResolution(
            profile=name_profile,
            source="name_fallback",
            resolver_version=PROFILE_RESOLVER_VERSION,
        )

    return ProfileResolution(
        profile="industrial",
        source="default_industrial",
        resolver_version=PROFILE_RESOLVER_VERSION,
    )


__all__ = ["COMP_TYPE_PROFILES", "PROFILE_KEYWORDS", "resolve_company_profile"]

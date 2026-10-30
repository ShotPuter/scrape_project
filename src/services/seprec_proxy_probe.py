"""SEPREC-specific proxy readiness and collector requests."""

from __future__ import annotations

from src.client.readiness import is_search_payload
from src.models.publication import PublicationSearchFilter
from src.proxy.probes import ProbeRequest, TargetProbe


def seprec_target_probe(api_base_url: str, search_path: str = "publicaciones") -> TargetProbe:
    base = api_base_url.rstrip("/")
    return TargetProbe(
        (
            ProbeRequest(
                "POST",
                f"{base}/{search_path}",
                is_search_payload,
                PublicationSearchFilter(page_size=10, category="1").to_api_payload(),
            ),
        )
    )

"""Extract code evidence, without confusing Official Gazette IDs with RES CVEs."""

from __future__ import annotations

import re
from urllib.parse import parse_qsl, urljoin, urlsplit

from bs4 import BeautifulSoup

from src.sites.dequienes.source import validate_cve

RES_HOST = "www.registrodeempresasysociedades.cl"


def extract_candidates(html: str, url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    for node in soup.select("script,style,nav,footer"):
        node.decompose()
    result = []
    seen = set()

    def add(code, kind, evidence):
        try:
            code = validate_cve(code)
        except ValueError:
            return
        key = code, kind, evidence
        if key not in seen:
            seen.add(key)
            result.append({"code": code, "kind": kind, "evidence": evidence[:500]})

    # The suffix of a Gazette URL identifies a publication, not a RES certificate.
    publication = re.search(r"-(\d{6,})/?$", urlsplit(url).path)
    if publication:
        add(publication[1], "diario_oficial", "publication URL: " + url)
    for anchor in soup.select("a[href]"):
        try:
            target = urljoin(url, str(anchor["href"]))
            parts = urlsplit(target)
            allowed = (
                parts.scheme == "https"
                and parts.hostname == RES_HOST
                and not parts.username
                and not parts.password
                and parts.port in (None, 443)
            )
        except ValueError:
            continue
        if not allowed:
            continue
        for name, code in parse_qsl(parts.query):
            if name.casefold() in {"cve", "codigoverificacion"}:
                add(code, "res_reference", "RES link: " + target)
    # Generic CVE labels remain ambiguous. Only an explicitly qualified RES label
    # routes to verification; even that is evidence, never a claim of validity.
    patterns = [
        (r"\b(?:CVE\s+(?:RES|\(RES\))|RES\s+CVE)\s*[:=]\s*([A-Za-z0-9]{6,64})\b", "res_reference"),
        (r"\bCVE\s*[:=]\s*([A-Za-z0-9]{6,64})\b", "ambiguous"),
    ]
    blocks = ["p", "li", "tr", "dd", "dt", "div", "section"]
    texts = {" ".join(node.get_text(" ", strip=True).split()) for node in soup.find_all(blocks) if node.find(blocks) is None}
    # Bare text labels are supported without joining unrelated neighbouring blocks.
    texts.update(str(node).strip() for node in soup.find_all(string=re.compile(r"\bCVE\b", re.IGNORECASE)))
    for text in sorted(texts):
        for pattern, kind in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                add(match[1], kind, text[max(0, match.start() - 50) : match.end() + 50])
    return result

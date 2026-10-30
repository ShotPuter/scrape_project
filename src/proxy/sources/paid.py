"""Local, private provider exports. Never send credentials to public list hosts."""

from pathlib import Path

from src.proxy.sources.catalog import ProxyCandidate
from src.utils.proxy_url import parse_proxy_url


def load_paid_candidates(path: Path) -> list[ProxyCandidate]:
    """Accept one proxy URL per line, including percent-encoded credentials."""
    candidates: dict[str, ProxyCandidate] = {}
    try:
        with path.open(encoding="utf-8-sig") as stream:
            for number, line in enumerate(stream, 1):
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                address = parse_proxy_url(line)
                if address is None:
                    raise ValueError(f"Invalid paid proxy on line {number}; expected scheme://[user:password@]host:port")
                candidates[address.url] = ProxyCandidate(address.url, address.scheme, "paid")
    except OSError as exc:
        raise ValueError("Cannot read PROXY_PAID_FILE") from exc
    return list(candidates.values())

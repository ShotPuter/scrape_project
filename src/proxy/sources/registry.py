"""Declarative HTTP/SOCKS list sources; no target-site or application imports."""

from dataclasses import dataclass

RAW = "https://raw.githubusercontent.com"


@dataclass(frozen=True, slots=True)
class SourceFile:
    url: str
    format: str = "json"
    protocol: str = "http"


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    files: tuple[SourceFile, ...]
    refresh_minutes: float


def protocol_files(repo: str, branch: str, names: tuple[str, str, str]) -> tuple[SourceFile, ...]:
    return tuple(
        SourceFile(f"{RAW}/{repo}/{branch}/{filename}", "text", protocol)
        for protocol, filename in zip(("http", "socks4", "socks5"), names, strict=True)
    )


SOURCE_REGISTRY = {
    "proxyscrape": SourceDefinition(
        (SourceFile("https://cdn.jsdelivr.net/gh/ProxyScrape/free-proxy-list@main/proxies/all/data.json"),), 5
    ),
    "hproxy": SourceDefinition((SourceFile("https://hproxy.com/api/proxy-list?format=json"),), 30),
    "proxifly": SourceDefinition((SourceFile(f"{RAW}/proxifly/free-proxy-list/main/proxies/all/data.json"),), 5),
    "monosans": SourceDefinition((SourceFile(f"{RAW}/monosans/proxy-list/main/proxies.json"),), 60),
    "thespeedx": SourceDefinition(
        protocol_files("TheSpeedX/PROXY-List", "master", ("http.txt", "socks4.txt", "socks5.txt")), 1440
    ),
    "jetkai": SourceDefinition((SourceFile(f"{RAW}/jetkai/proxy-list/main/online-proxies/json/proxies-advanced.json"),), 60),
    "iplocate": SourceDefinition((SourceFile(f"{RAW}/iplocate/free-proxy-list/main/all-proxies.txt", "text"),), 30),
    "vpslab": SourceDefinition(
        protocol_files("VPSLabCloud/VPSLab-Free-Proxy-List", "main", ("http_all.txt", "socks4_all.txt", "socks5_all.txt")),
        15,
    ),
    "thordata": SourceDefinition((SourceFile(f"{RAW}/Thordata/awesome-free-proxy-list/main/proxies/json/all.json"),), 360),
    "gfpcom": SourceDefinition(
        tuple(
            SourceFile(f"{RAW}/wiki/gfpcom/free-proxy-list/lists/{protocol}.txt", "text", protocol)
            for protocol in ("http", "socks4", "socks5")
        ),
        60,
    ),
    "dpangestuw": SourceDefinition(
        protocol_files("dpangestuw/Free-Proxy", "main", ("http_proxies.txt", "socks4_proxies.txt", "socks5_proxies.txt")), 5
    ),
    "rix4uni": SourceDefinition((SourceFile(f"{RAW}/rix4uni/fresh-proxy-list/main/proxylist.json"),), 10),
    "relayglass": SourceDefinition((SourceFile(f"{RAW}/relayglass/free-proxy-list/main/all.json"),), 5),
}

# Large or potentially stale reserves remain available through --sources / --provider all.
DEFAULT_SOURCES = ("proxyscrape", "hproxy", "proxifly", "monosans", "relayglass", "thordata", "vpslab")


def resolve_sources(value: str) -> list[str]:
    names = list(dict.fromkeys(part.strip().lower() for part in value.split(",") if part.strip()))
    if names == ["public"]:
        return list(DEFAULT_SOURCES)
    if names == ["all"]:
        return [*SOURCE_REGISTRY, "free-proxy"]
    unknown = [name for name in names if name not in SOURCE_REGISTRY and name not in {"free-proxy", "paid", "webshare"}]
    if unknown or not names:
        raise ValueError(f"Unknown or empty proxy sources: {', '.join(unknown) or value!r}")
    return names

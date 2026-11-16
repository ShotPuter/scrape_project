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
    # Text feeds keep protocol labels; an HTTPS-capable CONNECT proxy still
    # uses the HTTP proxy transport (TLS is established to the destination).
    "shiftytr": SourceDefinition(
        (
            *protocol_files("ShiftyTR/Proxy-List", "master", ("http.txt", "socks4.txt", "socks5.txt")),
            SourceFile(f"{RAW}/ShiftyTR/Proxy-List/master/https.txt", "text", "http"),
        ),
        60,
    ),
    "roosterkid": SourceDefinition(
        protocol_files("roosterkid/openproxylist", "main", ("HTTPS_RAW.txt", "SOCKS4_RAW.txt", "SOCKS5_RAW.txt")), 60
    ),
    "hideip": SourceDefinition(
        (
            *protocol_files("zloi-user/hideip.me", "main", ("http.txt", "socks4.txt", "socks5.txt")),
            SourceFile(f"{RAW}/zloi-user/hideip.me/main/https.txt", "text", "http"),
            SourceFile(f"{RAW}/zloi-user/hideip.me/main/connect.txt", "text", "http"),
        ),
        10,
    ),
    "clarketm": SourceDefinition((SourceFile(f"{RAW}/clarketm/proxy-list/master/proxy-list-raw.txt", "text"),), 1440),
    "maximilianfeix": SourceDefinition(
        (
            *protocol_files("maximilianfeix/free-proxy-list", "main", ("http.txt", "socks4.txt", "socks5.txt")),
            SourceFile(f"{RAW}/maximilianfeix/free-proxy-list/main/https.txt", "text", "http"),
        ),
        60,
    ),
    "mmpx12": SourceDefinition(
        (
            *protocol_files("mmpx12/proxy-list", "master", ("http.txt", "socks4.txt", "socks5.txt")),
            SourceFile(f"{RAW}/mmpx12/proxy-list/master/https.txt", "text", "http"),
        ),
        60,
    ),
    "aliilapro": SourceDefinition(protocol_files("ALIILAPRO/Proxy", "main", ("http.txt", "socks4.txt", "socks5.txt")), 60),
    "murongpig": SourceDefinition(
        protocol_files("MuRongPIG/Proxy-Master", "main", ("http.txt", "socks4.txt", "socks5.txt")), 60
    ),
    "zaeem20": SourceDefinition(
        (
            *protocol_files("Zaeem20/FREE_PROXIES_LIST", "master", ("http.txt", "socks4.txt", "socks5.txt")),
            SourceFile(f"{RAW}/Zaeem20/FREE_PROXIES_LIST/master/https.txt", "text", "http"),
        ),
        10,
    ),
    "vakhov": SourceDefinition(
        (
            *protocol_files("vakhov/fresh-proxy-list", "master", ("http.txt", "socks4.txt", "socks5.txt")),
            SourceFile(f"{RAW}/vakhov/fresh-proxy-list/master/https.txt", "text", "http"),
        ),
        60,
    ),
    "prxchk": SourceDefinition(protocol_files("prxchk/proxy-list", "main", ("http.txt", "socks4.txt", "socks5.txt")), 60),
    "hookzof": SourceDefinition((SourceFile(f"{RAW}/hookzof/socks5_list/master/proxy.txt", "text", "socks5"),), 60),
    "sunny9577": SourceDefinition((SourceFile(f"{RAW}/sunny9577/proxy-scraper/master/proxies.json"),), 60),
}

# Explicit public collection uses the broad catalog; the known stale Jetkai
# archive remains available via --sources all or --sources jetkai.
DEFAULT_SOURCES = tuple(name for name in SOURCE_REGISTRY if name != "jetkai")


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

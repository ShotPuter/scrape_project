"""Explicit provider adapters; downloads and verification remain shared."""

from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from typing import Any

from . import (
    dpangestuw,
    gfpcom,
    hproxy,
    iplocate,
    jetkai,
    monosans,
    proxifly,
    proxyscrape,
    relayglass,
    rix4uni,
    thespeedx,
    thordata,
    vpslab,
)

RowAdapter = Callable[[Mapping[str, Any], datetime], Iterator[dict[str, Any]]]
ADAPTERS: dict[str, RowAdapter] = {
    "proxyscrape": proxyscrape.adapt,
    "hproxy": hproxy.adapt,
    "proxifly": proxifly.adapt,
    "monosans": monosans.adapt,
    "thespeedx": thespeedx.adapt,
    "jetkai": jetkai.adapt,
    "iplocate": iplocate.adapt,
    "vpslab": vpslab.adapt,
    "thordata": thordata.adapt,
    "gfpcom": gfpcom.adapt,
    "dpangestuw": dpangestuw.adapt,
    "rix4uni": rix4uni.adapt,
    "relayglass": relayglass.adapt,
}


def adapt_rows(row: Mapping[str, Any], source: str, now: datetime) -> Iterator[dict[str, Any]]:
    adapter = ADAPTERS.get(source)
    if adapter is None:
        yield dict(row)
    else:
        yield from adapter(row, now)

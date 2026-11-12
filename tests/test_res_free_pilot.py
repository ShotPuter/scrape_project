"""Tests for the free pilot's cache logic: no browser, no network, no CAPTCHA."""

from __future__ import annotations

from collections import Counter

import pytest

from scripts import res_free_pilot as pilot
from src.sites.res.source import SearchIssue

CVE = "ACuKDHhqsvAd"
CODIGO = "49-AB-92-7F-87-8A-65-59-67-3F-3E-D3-3B-19-26-0D"
LINK = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={CVE}&codigo={CODIGO}"
NEW_CODIGO = "01-5B-0F-AA-0E-22-BC-56-BD-78-6F-3D-98-9B-63-41"
NEW_LINK = f"https://www.registrodeempresasysociedades.cl/Certificados/Escritura.aspx?cve={CVE}&codigo={NEW_CODIGO}"
PDF = b"%PDF-1.4\nexample\n%%EOF"


class FakeCascade:
    def __init__(self):
        self.calls = []

    async def mint(self, url, fill=None, **kwargs):
        self.calls.append(url)
        return "token"


def make_args(tmp_path, **overrides):
    args = pilot.parse_args(["--output", str(tmp_path), "--audio-model", ""])
    args.state_path = tmp_path / "state.json"
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def state_with(cves=None, companies=None):
    return {"cves": cves or {}, "companies": companies or {}}


async def test_cached_codigo_downloads_without_a_token(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    state = state_with({CVE: {"cve": CVE, "codigo": CODIGO, "link": LINK}})
    cascade = FakeCascade()
    counters = Counter()
    monkeypatch.setattr(pilot.http_flow, "download", lambda session, link: PDF)

    document = await pilot.ensure_document(cascade, None, CVE, None, state, args, tmp_path, counters)

    assert cascade.calls == []
    assert document["cve"] == CVE
    assert document["status"] == "downloaded"
    assert counters["downloaded"] == 1
    assert (tmp_path / document["local_path"]).read_bytes() == PDF


async def test_second_run_reuses_the_stored_pdf(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    state = state_with({CVE: {"cve": CVE, "codigo": CODIGO, "link": LINK}})
    cascade = FakeCascade()
    counters = Counter()
    restored = pilot.persist_pdf(tmp_path, CVE, PDF, LINK)

    document = await pilot.ensure_document(cascade, None, CVE, None, state, args, tmp_path, counters)

    assert cascade.calls == []
    assert document["reused"] is True
    assert document["local_path"] == restored["local_path"]
    assert counters["reused"] == 1
    assert counters["downloaded"] == 0


async def test_existing_pdf_survives_missing_codigo_state_without_network(tmp_path, monkeypatch):
    args = make_args(tmp_path)
    restored = pilot.persist_pdf(tmp_path, CVE, PDF, LINK)
    path = tmp_path / restored["local_path"]
    before = path.stat().st_mtime_ns
    cascade = FakeCascade()

    def forbidden(*args):
        pytest.fail("A valid local PDF must not be downloaded again")

    monkeypatch.setattr(pilot.http_flow, "download", forbidden)
    document = await pilot.ensure_document(cascade, None, CVE, None, state_with(), args, tmp_path, Counter())
    assert document["reused"] is True
    assert path.stat().st_mtime_ns == before
    assert cascade.calls == []


async def test_cached_codigo_is_preserved_after_connection_failure(tmp_path, monkeypatch):
    args = make_args(tmp_path)
    state = state_with({CVE: {"codigo": CODIGO, "link": LINK}})
    cascade = FakeCascade()

    def download(*args):
        raise SearchIssue("proxy_failed", "Temporary connection failure")

    monkeypatch.setattr(pilot.http_flow, "download", download)
    with pytest.raises(SearchIssue) as error:
        await pilot.ensure_document(cascade, None, CVE, None, state, args, tmp_path, Counter())
    assert error.value.status == "proxy_failed"
    assert state["cves"][CVE]["codigo"] == CODIGO
    assert cascade.calls == []


async def test_token_form_waits_for_widget_without_waiting_for_dom_load():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    widget = SimpleNamespace(wait_for=AsyncMock())
    frame = SimpleNamespace(locator=Mock(return_value=widget))
    page = SimpleNamespace(
        url=pilot.FORM_URL, goto=AsyncMock(return_value=SimpleNamespace(status=200)), frame_locator=Mock(return_value=frame)
    )
    await pilot.open_token_form(page, pilot.FORM_URL, 40)
    assert page.goto.call_args.kwargs["wait_until"] == "commit"
    assert widget.wait_for.call_args.kwargs["state"] == "visible"


async def test_token_form_requires_widget_readiness():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    widget = SimpleNamespace(wait_for=AsyncMock(side_effect=TimeoutError("widget unavailable")))
    page = SimpleNamespace(
        url=pilot.FORM_URL,
        goto=AsyncMock(return_value=SimpleNamespace(status=200)),
        frame_locator=Mock(return_value=SimpleNamespace(locator=Mock(return_value=widget))),
    )
    with pytest.raises(SearchIssue) as error:
        await pilot.open_token_form(page, pilot.FORM_URL, 40)
    assert error.value.status == "proxy_failed"


async def test_cache_miss_verifies_once_and_then_downloads_for_free(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    state = state_with()
    cascade = FakeCascade()
    counters = Counter()
    seen = {"verify": 0}

    def verify(session, cve, token):
        seen["verify"] += 1
        assert token == "token"
        return cve, CODIGO, LINK

    monkeypatch.setattr(pilot.http_flow, "verify", verify)
    monkeypatch.setattr(pilot.http_flow, "download", lambda session, link: PDF)

    document = await pilot.ensure_document(cascade, None, CVE, "rut:1", state, args, tmp_path, counters)
    again = await pilot.ensure_document(cascade, None, CVE, "rut:1", state, args, tmp_path, counters)

    assert len(cascade.calls) == 1
    assert cascade.calls[0].endswith("/VerificarCertificados.aspx")
    assert seen["verify"] == 1
    assert state["cves"][CVE]["codigo"] == CODIGO
    assert state["cves"][CVE]["company_id"] == "rut:1"
    assert pilot.load_state(args.state_path)["cves"][CVE]["codigo"] == CODIGO
    assert document["status"] == "downloaded"
    assert again["reused"] is True
    assert counters["codigos"] == 1
    assert counters["downloaded"] == 1
    assert counters["reused"] == 1


async def test_stale_codigo_is_replaced_by_a_new_verification(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path)
    state = state_with({CVE: {"cve": CVE, "codigo": CODIGO, "link": LINK}})
    cascade = FakeCascade()
    counters = Counter()
    attempts = {"download": 0}

    def download(session, link):
        attempts["download"] += 1
        if attempts["download"] == 1:
            raise SearchIssue("retry_wait", "Certificate HTTP 302")
        return PDF

    monkeypatch.setattr(pilot.http_flow, "download", download)
    monkeypatch.setattr(pilot.http_flow, "verify", lambda session, cve, token: (cve, NEW_CODIGO, NEW_LINK))

    document = await pilot.ensure_document(cascade, None, CVE, None, state, args, tmp_path, counters)

    assert len(cascade.calls) == 1
    assert attempts["download"] == 2
    assert state["cves"][CVE]["codigo"] == NEW_CODIGO
    assert document["status"] == "downloaded"
    assert counters["codigos"] == 1


async def test_company_with_cached_search_spends_no_token(tmp_path, monkeypatch) -> None:
    args = make_args(tmp_path, max_documents=1)
    company = {"company_id": "rut:78054070-2", "company_name": "AGRICOLA NANCAGUA SpA", "rut": "78054070-2"}
    state = state_with(
        {CVE: {"cve": CVE, "codigo": CODIGO, "link": LINK}}, {"rut:78054070-2": {"cves": [CVE], "status": "completed"}}
    )
    cascade = FakeCascade()
    counters = Counter()
    monkeypatch.setattr(pilot.http_flow, "download", lambda session, link: PDF)

    result = await pilot.process_company(cascade, None, company, state, args, tmp_path, counters)

    assert cascade.calls == []
    assert result["status"] == "downloaded"
    assert result["search_status"] == "completed"
    assert [document["cve"] for document in result["documents"]] == [CVE]


async def test_http_only_cascade_never_mints() -> None:
    cascade = pilot.HttpOnlyCascade(Counter(), [])

    with pytest.raises(SearchIssue) as error:
        await cascade.mint(pilot.RES_URL)

    assert error.value.status == "captcha_required"


def test_state_round_trip_and_corrupt_file(tmp_path) -> None:
    path = tmp_path / "state.json"
    assert pilot.load_state(path) == {"cves": {}, "companies": {}}

    pilot.save_state(path, {"cves": {CVE: {"codigo": CODIGO}}, "companies": {}})
    assert pilot.load_state(path)["cves"][CVE]["codigo"] == CODIGO
    path.write_text("{not json", encoding="utf-8")
    assert pilot.load_state(path) == {"cves": {}, "companies": {}}


async def test_brightdata_keeps_one_authenticated_route_and_ca_for_both_transports(tmp_path, monkeypatch):
    from scripts import res_audio_pilot

    args = make_args(tmp_path, proxy_provider="brightdata", proxy_limit=2)
    created = []

    def proxy(output):
        assert output == tmp_path
        index = len(created) + 1
        created.append(index)
        return (
            {
                "server": "http://brd.superproxy.io:44445",
                "username": f"test-session-{index}",
                "password": "private-test-password",
            },
            f"192.0.2.{index}",
            "unused-browser-flag",
        )

    async def immediate(function, *args):
        return function(*args)

    monkeypatch.setattr(res_audio_pilot, "brightdata_proxy", proxy)
    monkeypatch.setattr(pilot.asyncio, "to_thread", immediate)
    endpoints = await pilot.load_proxy_endpoints(args)
    assert len(endpoints) == 2
    assert endpoints[0].browser["username"] in endpoints[0].url
    assert endpoints[0].expected_ip == "192.0.2.1"
    assert endpoints[1].expected_ip == "192.0.2.2"
    assert endpoints[0].ca_file.name == "brightdata-proxy-ca.crt"
    assert "private-test-password" not in repr(endpoints)


async def test_http_only_keeps_explicit_proxy_provider(tmp_path, monkeypatch):
    args = make_args(tmp_path, http_only=True, proxy_provider="residential")
    endpoint = pilot.endpoint_from_url("http://user:private-password@192.0.2.9:8080")
    monkeypatch.setattr(pilot, "load_proxy_file", lambda p: [endpoint])
    assert await pilot.load_proxy_endpoints(args) == [endpoint]


async def test_company_completion_does_not_double_count_downloaded_documents(tmp_path, monkeypatch):
    from collections import deque
    from types import SimpleNamespace

    args = make_args(tmp_path)
    counters = Counter()

    async def process(*values):
        values[-1]["downloaded"] += 1
        return {"status": "downloaded", "documents": []}

    monkeypatch.setattr(pilot, "process_company", process)
    queue = deque([({"company_id": "example", "company_name": "Example"}, 0)])
    stats = pilot.new_stats(0, pilot.DIRECT)
    await pilot.drain(
        SimpleNamespace(minted=0),
        None,
        queue,
        deque(),
        state_with(),
        args,
        counters,
        {"run_id": "test"},
        stats,
        {},
        {"stop": False, "failures": 0},
    )
    assert counters["downloaded"] == 1
    assert counters["company_status:downloaded"] == 1


async def test_navigation_timeout_is_a_proxy_failure_available_for_rotation(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    args = make_args(tmp_path)
    page = SimpleNamespace(goto=AsyncMock(side_effect=TimeoutError("navigation timed out")))
    cascade = pilot.Cascade(page, args, "", None, Counter(), [])
    with pytest.raises(SearchIssue) as error:
        await cascade.mint(pilot.RES_URL)
    assert error.value.status == "proxy_failed"


async def test_transient_brightdata_setup_failure_does_not_discard_other_routes(tmp_path, monkeypatch):
    from scripts import res_audio_pilot

    args = make_args(tmp_path, proxy_provider="brightdata", proxy_limit=2)
    calls = []

    def proxy(_):
        calls.append(1)
        if len(calls) == 1:
            raise res_audio_pilot.ProxyPreflightError("proxy_connect_failed", "Provider unavailable")
        return (
            {"server": "http://brd.superproxy.io:44445", "username": "test", "password": "private"},
            "192.0.2.1",
            "unused-flag",
        )

    async def immediate(function, *values):
        return function(*values)

    monkeypatch.setattr(res_audio_pilot, "brightdata_proxy", proxy)
    monkeypatch.setattr(pilot.asyncio, "to_thread", immediate)
    endpoints = await pilot.load_proxy_endpoints(args)
    assert len(endpoints) == 1 and endpoints[0].expected_ip == "192.0.2.1"
    assert args.proxy_setup_failures == [{"slot": 1, "code": "proxy_connect_failed"}]


async def test_untried_companies_precede_retries_and_attempts_are_checkpointed(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    args = make_args(tmp_path, company_attempts=2, challenges_per_proxy=1, cve=None)
    companies = [{"company_id": key, "company_name": key, "rut": None} for key in ["A", "B"]]
    seen = []

    @asynccontextmanager
    async def session(*values):
        yield SimpleNamespace(minted=0, events=[], audio=None), None

    async def process(cascade, http, company, *values):
        seen.append(company["company_id"])
        if company["company_id"] == "A":
            raise SearchIssue("captcha_required", "Google declined audio")
        return {**company, "status": "not_found", "documents": []}

    monkeypatch.setattr(pilot, "load_companies", lambda *a: companies)
    monkeypatch.setattr(pilot, "endpoint_session", session)
    monkeypatch.setattr(pilot, "process_company", process)
    report = {"run_id": "ordered", "proxies": []}
    await pilot.scan([pilot.DIRECT, pilot.DIRECT], args, state_with(), Counter(), report, None, "", [])
    assert seen == ["A", "B", "A"]
    assert [item["result"]["status"] for item in report["attempts"]] == ["captcha_required", "not_found", "captcha_required"]
    checkpoint = pilot.json.loads((tmp_path / "report-ordered.json").read_text())
    assert len(checkpoint["companies"]) == 2
    assert checkpoint["progress"]["completed_companies"] == 2


@pytest.mark.parametrize(
    "dynamic,browser_ip,accepted", [(True, "192.0.2.2", True), (True, "192.0.2.3", False), (False, "192.0.2.2", False)]
)
async def test_current_browser_http_pair_must_match_even_after_dynamic_preflight_change(
    tmp_path, monkeypatch, dynamic, browser_ip, accepted
):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    args = make_args(tmp_path)
    endpoint = pilot.ProxyEndpoint(
        "http://192.0.2.1:8080", {"server": "http://192.0.2.1:8080"}, "test", expected_ip="192.0.2.1", dynamic_ip=dynamic
    )
    http = SimpleNamespace(close=Mock())
    page = SimpleNamespace(close=AsyncMock(), route=AsyncMock())

    @asynccontextmanager
    async def browser(*args, **kwargs):
        yield SimpleNamespace(new_page=AsyncMock(return_value=page))

    async def immediate(function, *values):
        return function(*values)

    monkeypatch.setattr(pilot.http_flow, "new_session", lambda **k: http)
    monkeypatch.setattr(pilot.http_flow, "prepare", lambda h: None)
    monkeypatch.setattr(pilot, "http_exit_ip", lambda h: "192.0.2.2")
    monkeypatch.setattr(pilot, "browser_exit_ip", AsyncMock(return_value=browser_ip))
    monkeypatch.setattr(pilot, "launch_context", browser)
    monkeypatch.setattr(pilot.asyncio, "to_thread", immediate)
    stats = pilot.new_stats(0, endpoint)
    try:
        async with pilot.endpoint_session(0, endpoint, args, Counter(), [], None, "", stats):
            assert accepted
            assert stats["http_ip"] == stats["browser_ip"] == "192.0.2.2"
            assert stats["preflight_ip_changed"]
    except SearchIssue as error:
        assert not accepted and error.status == "proxy_ip_mismatch"
    http.close.assert_called_once()


@pytest.mark.parametrize(
    "url,kind,blocked",
    [
        (f"https://{pilot.HOST}/js/jquery-ui-1.8.10.custom.min.js", "script", True),
        (f"https://{pilot.HOST}/Content/Styles.css", "stylesheet", True),
        (pilot.FORM_URL, "document", False),
        ("https://www.google.com/recaptcha/api.js", "script", False),
        ("https://www.gstatic.com/recaptcha/releases/version/recaptcha.js", "script", False),
        ("https://www.google.com/recaptcha/api2/anchor", "document", False),
        ("https://www.google.com/recaptcha/api2/payload", "image", False),
    ],
)
async def test_token_asset_filter_keeps_form_and_all_challenge_requests(url, kind, blocked):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    route = SimpleNamespace(request=SimpleNamespace(url=url, resource_type=kind), abort=AsyncMock(), continue_=AsyncMock())
    await pilot.token_asset_route(route)
    assert route.abort.await_count == int(blocked)
    assert route.continue_.await_count == int(not blocked)


async def test_lazy_proxy_budget_does_not_preflight_until_a_session_is_needed(tmp_path, monkeypatch):
    from scripts import res_audio_pilot

    args = make_args(tmp_path, proxy_provider="brightdata", proxy_limit=100, lazy_proxies=True)
    calls = []

    def proxy(_):
        calls.append(1)
        return (
            {"server": "http://brd.superproxy.io:44445", "username": "fake-session", "password": "fake"},
            "192.0.2.1",
            "unused",
        )

    async def immediate(function, *values):
        return function(*values)

    monkeypatch.setattr(pilot.asyncio, "to_thread", immediate)
    monkeypatch.setattr(res_audio_pilot, "brightdata_proxy", proxy)
    endpoints = await pilot.load_proxy_endpoints(args)
    assert len(endpoints) == 100 and calls == []
    endpoint = await pilot.resolve_proxy(endpoints[0], args)
    assert calls == [1] and endpoint.expected_ip == "192.0.2.1"


def test_brightdata_runtime_environment_overrides_local_file(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import requests

    from scripts import res_audio_pilot

    monkeypatch.setattr(
        res_audio_pilot,
        "dotenv_values",
        lambda _: {"BRIGHTDATA_PROXY_USERNAME": "file-user", "BRIGHTDATA_PROXY_PASSWORD": "file-password"},
    )
    monkeypatch.setenv("BRIGHTDATA_PROXY_USERNAME", "runtime-user")
    monkeypatch.setenv("BRIGHTDATA_PROXY_PASSWORD", "runtime-password")
    monkeypatch.setenv("BRIGHTDATA_PROXY_SERVER", "http://proxy.invalid:44445")
    monkeypatch.setattr(res_audio_pilot.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=b"public-fixture"))

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def get(self, url, **kwargs):
            assert "runtime-user" in kwargs["proxies"]["https"]
            assert "runtime-password" in kwargs["proxies"]["https"]
            assert "file-user" not in kwargs["proxies"]["https"]
            return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"ip": "192.0.2.1"})

    monkeypatch.setattr(requests, "Session", Session)
    settings, expected, _ = res_audio_pilot.brightdata_proxy(tmp_path)
    assert settings["password"] == "runtime-password" and expected == "192.0.2.1"


async def test_failure_limit_stops_before_spending_more_proxy_sessions(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    args = make_args(tmp_path, company_attempts=2, max_failures=1, cve=None)
    companies = [{"company_id": key, "company_name": key, "rut": None} for key in ["A", "B"]]
    sessions = []

    @asynccontextmanager
    async def session(*values):
        sessions.append(1)
        yield SimpleNamespace(minted=0, events=[], audio=None), None

    async def fail(*values):
        raise SearchIssue("proxy_failed", "Temporary network failure")

    monkeypatch.setattr(pilot, "load_companies", lambda *a: companies)
    monkeypatch.setattr(pilot, "endpoint_session", session)
    monkeypatch.setattr(pilot, "process_company", fail)
    report = {"run_id": "bounded", "proxies": []}
    await pilot.scan([pilot.DIRECT] * 10, args, state_with(), Counter(), report, None, "", [])
    assert sessions == [1]
    assert len(report["attempts"]) == 1
    assert {r["status"] for r in report["companies"]} == {"stopped"}


async def test_provider_authentication_refusal_stops_session_creation(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    from scripts.res_audio_pilot import ProxyPreflightError

    args = make_args(tmp_path, cve=None)
    monkeypatch.setattr(pilot, "load_companies", lambda *a: [{"company_id": "A", "company_name": "A"}])
    resolve = AsyncMock(side_effect=ProxyPreflightError("ip_blacklisted", "Provider rejected client IP"))
    monkeypatch.setattr(pilot, "resolve_proxy", resolve)
    report = {"run_id": "auth", "proxies": []}
    await pilot.scan([pilot.DIRECT] * 100, args, state_with(), Counter(), report, None, "", [])
    assert resolve.await_count == 1
    assert report["proxies"][0]["status"] == "proxy_auth_failed"

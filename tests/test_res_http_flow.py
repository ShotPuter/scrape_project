"""Unit tests for the plain-HTTP RES flow: no network, no browser, no CAPTCHA."""

from __future__ import annotations

import json

import httpx
import pytest

from src.sites.res import http_flow

CVE = "ACuKDHhqsvAd"
CODIGO = "49-AB-92-7F-87-8A-65-59-67-3F-3E-D3-3B-19-26-0D"
COMPANY_LINK = "/Busqueda/ActuacionDetalle.aspx?TipoBusqueda=1&ParamBusqueda1=78054070&codigo=abc123"
DOCUMENT_LINK = f"/Certificados/Escritura.aspx?cve={CVE}&codigo={CODIGO.lower()}"
ACTIONS = [
    {
        "run": "78054070-2",
        "rso_razonsocial": "AGRICOLA NANCAGUA SpA",
        "rso_fecha": "01-06-2020 12:00:00",
        "tac_glosa": "Constitucion",
        "rso_numeroatencion": "12345",
        "frec_glosa": CVE,
    }
]


def envelope(payload):
    return {"d": json.dumps(payload)}


def client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)


@pytest.mark.parametrize("failure", ["connection", 502, 503, 504])
def test_get_retries_transient_failure_once(failure, monkeypatch):
    seen = []
    monkeypatch.setattr(http_flow.time, "sleep", lambda _: None)

    def handler(request):
        seen.append(request)
        if len(seen) == 1:
            if failure == "connection":
                raise httpx.ConnectError("temporary DNS failure", request=request)
            return httpx.Response(failure)
        return httpx.Response(200)

    with client(handler) as session:
        assert http_flow.get(session, http_flow.FORM_URL).status_code == 200
    assert len(seen) == 2


def test_get_connection_retries_are_bounded(monkeypatch):
    seen = []
    monkeypatch.setattr(http_flow.time, "sleep", lambda _: None)

    def handler(request):
        seen.append(request)
        raise httpx.ConnectError("temporary DNS failure", request=request)

    with client(handler) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.get(session, http_flow.FORM_URL)
    assert error.value.status == "proxy_failed"
    assert len(seen) == 2


@pytest.mark.parametrize("status", [403, 429])
def test_get_does_not_retry_access_denial(status):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(status)

    with client(handler) as session:
        assert http_flow.get(session, http_flow.FORM_URL).status_code == status
    assert len(seen) == 1


def test_post_connection_failure_never_replays_the_token():
    seen = []

    def handler(request):
        seen.append(request)
        raise httpx.ConnectError("temporary DNS failure", request=request)

    with client(handler) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.search(session, token="single-use", rut="78054070-2")
    assert error.value.status == "proxy_failed"
    assert len(seen) == 1


def ok(handler):
    """Route the four known paths, record requests, fail loudly on anything else."""

    def route(request):
        path = request.url.path
        if path in handler:
            return handler[path](request)
        raise AssertionError(f"unexpected request: {request.method} {path}")

    return client(route)


def test_search_details_verify_download() -> None:
    requests = []

    def search(request):
        requests.append(request)
        return httpx.Response(200, json=envelope({"Link": COMPANY_LINK}))

    def details(request):
        requests.append(request)
        return httpx.Response(200, json=envelope(ACTIONS))

    def verify(request):
        requests.append(request)
        return httpx.Response(200, json=envelope({"Link": DOCUMENT_LINK}))

    def download(request):
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "application/pdf"}, content=b"%PDF-1.4\n%%EOF")

    handler = {
        "/BuscarActuaciones2.aspx/Busqueda": search,
        "/Busqueda/ActuacionDetalle.aspx/DatosActuaciones": details,
        "/VerificarCertificados.aspx/Busqueda": verify,
        "/Certificados/Escritura.aspx": download,
    }
    with ok(handler) as session:
        link = http_flow.search(session, token="token-1", rut="78054070-2")
        actions = http_flow.details(session, link)
        cve, codigo, document_link = http_flow.verify(session, CVE, "token-2")
        data = http_flow.download(session, document_link)

    assert link.endswith(COMPANY_LINK)
    assert actions == [
        {
            "rut": "78054070-2",
            "company_name": "AGRICOLA NANCAGUA SpA",
            "date": "2020-06-01",
            "event_type": "Constitucion",
            "attention_number": "12345",
            "cve": CVE,
        }
    ]
    assert (cve, codigo) == (CVE, CODIGO)
    assert data.startswith(b"%PDF-")

    bodies = {request.url.path: json.loads(request.content) for request in requests if request.method == "POST"}
    assert bodies["/BuscarActuaciones2.aspx/Busqueda"] == {
        "tipoRegistro": "1",
        "tipoBusqueda": "1",
        "texto": "78054070-2",
        "Response": "token-1",
    }
    assert bodies["/Busqueda/ActuacionDetalle.aspx/DatosActuaciones"] == {
        "tipoBusqueda": "1",
        "ParamBusqueda1": "78054070",
        "codehash": "abc123",
    }
    assert bodies["/VerificarCertificados.aspx/Busqueda"] == {"Response": "token-2", "texto": CVE}


def test_search_by_name_uses_backend_code_three() -> None:
    seen = {}

    def search(request):
        seen.update(json.loads(request.content))
        return httpx.Response(200, json=envelope({"Link": COMPANY_LINK}))

    with ok({"/BuscarActuaciones2.aspx/Busqueda": search}) as session:
        http_flow.search(session, token="token", name="AGRICOLA NANCAGUA SpA")

    assert seen["tipoBusqueda"] == "3"
    assert seen["texto"] == "AGRICOLA NANCAGUA SpA"


def test_verify_error_message_asks_for_a_new_token() -> None:
    def verify(request):
        return httpx.Response(200, json=envelope({"ErrorMessage": "reCAPTCHA verification failed"}))

    with ok({"/VerificarCertificados.aspx/Busqueda": verify}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.verify(session, CVE, "token")

    assert error.value.status == "captcha_required"


def test_verify_rejects_an_unexpected_codigo() -> None:
    bad_link = f"/Certificados/Escritura.aspx?cve={CVE}&codigo=not-a-codigo"

    def verify(request):
        return httpx.Response(200, json=envelope({"Link": bad_link}))

    with ok({"/VerificarCertificados.aspx/Busqueda": verify}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.verify(session, CVE, "token")

    assert error.value.status == "retry_wait"


def test_verify_requires_a_document_link() -> None:
    def verify(request):
        return httpx.Response(200, json=envelope({"Link": "/Default.aspx"}))

    with ok({"/VerificarCertificados.aspx/Busqueda": verify}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.verify(session, CVE, "token")

    assert error.value.status == "retry_wait"


@pytest.mark.parametrize(("status", "expected"), [(302, "retry_wait"), (403, "blocked"), (429, "blocked")])
def test_download_maps_http_failures(status: int, expected: str) -> None:
    def download(request):
        return httpx.Response(status, headers={"location": "/"})

    with ok({"/Certificados/Escritura.aspx": download}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.download(session, DOCUMENT_LINK)

    assert error.value.status == expected


def test_download_rejects_html_instead_of_a_pdf() -> None:
    def download(request):
        return httpx.Response(200, headers={"content-type": "text/html"}, content=b"<html></html>")

    with ok({"/Certificados/Escritura.aspx": download}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.download(session, DOCUMENT_LINK)

    assert error.value.status == "download_failed"
    assert error.value.diagnostic["http_status"] == 200
    assert error.value.diagnostic["path"] == "/Certificados/Escritura.aspx"
    assert CODIGO not in json.dumps(error.value.diagnostic)


def test_prepare_maps_rate_limiting_to_blocked() -> None:
    def form(request):
        return httpx.Response(429)

    with (
        ok({"/BuscarActuaciones2.aspx": form, "/VerificarCertificados.aspx": form}) as session,
        pytest.raises(http_flow.SearchIssue) as error,
    ):
        http_flow.prepare(session)

    assert error.value.status == "blocked"


def test_search_rejects_non_json() -> None:
    def search(request):
        return httpx.Response(200, text="<html>maintenance</html>")

    with ok({"/BuscarActuaciones2.aspx/Busqueda": search}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.search(session, token="token", rut="78054070-2")

    assert error.value.status == "retry_wait"


def test_details_rejects_a_link_without_a_codehash() -> None:
    with ok({}) as session, pytest.raises(http_flow.SearchIssue) as error:
        http_flow.details(session, "/Busqueda/ActuacionDetalle.aspx?TipoBusqueda=1&ParamBusqueda1=78054070")

    assert error.value.status == "retry_wait"


def test_connection_establishment_retry_sends_single_use_post_once(monkeypatch):
    import socket
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    received = []
    dns_calls = []
    original = socket.getaddrinfo

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            data = json.dumps(envelope({"Link": COMPANY_LINK})).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()

    def resolve(*args, **kwargs):
        dns_calls.append(1)
        if len(dns_calls) == 1:
            raise socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
        return original(*args, **kwargs)

    try:
        monkeypatch.setattr(socket, "getaddrinfo", resolve)
        # Exercise the actual transport with a local HTTP fixture, without RES traffic.
        with http_flow.new_session(timeout=2) as session:
            response = session.post(f"http://127.0.0.1:{server.server_port}/search", json={"Response": "single-use"})
        assert response.status_code == 200
        assert dns_calls == [1, 1]
        assert received == [{"Response": "single-use"}]
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def test_verify_rejects_a_document_link_for_a_different_cve():
    link = DOCUMENT_LINK.replace(CVE, "ACfJoaiSFQ8v")
    with (
        client(lambda _: httpx.Response(200, json=envelope({"Link": link}))) as session,
        pytest.raises(http_flow.SearchIssue, match="different CVE"),
    ):
        http_flow.verify(session, CVE, "single-use")


def test_unexpected_response_diagnostics_bound_the_body_sample():
    body = b"<html><title>Temporary failure</title>" + b"x" * 100000
    with (
        client(lambda _: httpx.Response(200, headers={"content-type": "text/html"}, content=body)) as session,
        pytest.raises(http_flow.DownloadIssue) as failed,
    ):
        http_flow.download(session, DOCUMENT_LINK)
    assert len(failed.value.body_sample) == 65536
    assert failed.value.diagnostic["sample_truncated"] is True
    assert failed.value.diagnostic["body_bytes"] == len(body)

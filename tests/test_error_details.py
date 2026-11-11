import asyncio

from src.sites.dequienes.hybrid import BootstrapFailure
from src.utils.error_details import exception_details, failure_state, safe_message


def test_error_summaries_hide_tokens_proxy_credentials_and_query_strings(monkeypatch):
    monkeypatch.setenv("WEBSHARE_API_TOKEN", "secret-key")
    text = safe_message(
        "secret-key https://user:password@proxy.example:80/path?token=secret "
        "https://api.telegram.org/bot123:secret/sendMessage?chat_id=123"
    )
    for value in ["secret-key", "password", "123:secret", "token=secret", "chat_id=123"]:
        assert value not in text


def test_exception_groups_expose_the_real_cause_and_distinguish_cancellation():
    error = ExceptionGroup("unhandled errors in a TaskGroup", [BootstrapFailure("Sitemap HTTP 403", state="blocked")])
    details = exception_details(error)
    assert "Sitemap HTTP 403" in details["summary"]
    assert failure_state(error) == "blocked"
    assert failure_state(TimeoutError("navigation timed out")) == "failed"
    assert failure_state(asyncio.CancelledError()) == "interrupted"

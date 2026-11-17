
import httpx

from src.sites.res import open_data
from src.sites.res.free_stream import parse_args


def test_archive_orders_months_and_years_and_reads_inactive_csv():
    def route(request):
        if request.url.path.endswith("package_show"):
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "result": {
                        "resources": [
                            {
                                "name": "Constituciones 2026 septiembre 2026",
                                "id": "new",
                                "format": "csv",
                                "datastore_active": True,
                            },
                            {
                                "name": "Constituciones 2025",
                                "id": "old",
                                "format": "csv",
                                "datastore_active": False,
                                "url": "https://datos.gob.cl/old.csv",
                            },
                        ]
                    },
                },
            )
        if request.url.path == "/old.csv":
            return httpx.Response(
                200, text="RUT;Razon Social;Anio;Mes\n76615169-8;Duplicate;2025;Diciembre\n78081198-6;Old;2025;Enero\n"
            )
        rows = [
            {"RUT": "78081269-9", "Razon Social": "January", "Anio": 2026, "Mes": "Enero"},
            {"RUT": "76615169-8", "Razon Social": "Latest", "Anio": 2026, "Mes": "Septiembre"},
        ]
        return httpx.Response(200, json={"success": True, "result": {"records": rows, "total": 2}})

    with httpx.Client(transport=httpx.MockTransport(route)) as session:
        result = open_data.select_archive(2026, session=session)
    assert [row["company_name"] for row in result["companies"]] == ["Latest", "January", "Old"]
    assert result["newest_period"] == "2026-09" and result["oldest_period"] == "2025-01"
    assert len({row["company_id"] for row in result["companies"]}) == 3


def test_archive_allows_all_companies_and_ten_browsers():
    args = parse_args(["--archive-from-year", "2026", "--limit", "0", "--browsers", "10"])
    assert args.limit == 0 and args.browsers == 10 and args.telegram_events == "summary"


def test_archive_reads_three_years_concurrently_and_yields_newest_first(monkeypatch):
    from threading import Barrier

    all_started = Barrier(3, timeout=3)

    def read_year(session, year, resource):
        all_started.wait()
        return {1: [resource]}

    monkeypatch.setattr(open_data, "read_archive_year", read_year)
    resources = {2024: "old", 2025: "middle", 2026: "new"}
    assert list(open_data.archive_years(None, resources, workers=3)) == [
        (2026, {1: ["new"]}),
        (2025, {1: ["middle"]}),
        (2024, {1: ["old"]}),
    ]

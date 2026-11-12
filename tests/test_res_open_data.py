import json

import httpx
import pytest

from src.sites.res.open_data import select_companies


def test_official_selection_paginates_and_deduplicates_ruts():
    offsets = []
    rows = [
        {"RUT": "78081182-K", "Razon Social": "Company A", "Anio": "2025", "Mes": "Enero"},
        {"RUT": "78081182-K", "Razon Social": "Company A duplicate", "Anio": "2025", "Mes": "Enero"},
        {"RUT": "78081198-6", "Razon Social": "Company B", "Anio": "2025", "Mes": "Enero"},
    ]

    def handler(request):
        if request.url.path.endswith("package_show"):
            result = {
                "resources": [{"id": "official-2025", "name": "Constituciones del año 2025", "datastore_active": True}]
            }
        else:
            assert json.loads(request.url.params["filters"]) == {"Mes": "Enero"}
            assert request.url.params["sort"] == "_id asc"
            offset = int(request.url.params["offset"])
            offsets.append(offset)
            result = {"records": rows[offset : offset + int(request.url.params["limit"])], "total": 3}
        return httpx.Response(200, json={"success": True, "result": result})

    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        result = select_companies(2025, 1, 2, session=session)
    assert offsets == [0, 2]
    assert result["resource_id"] == "official-2025"
    assert [c["company_name"] for c in result["companies"]] == ["Company A", "Company B"]


@pytest.mark.parametrize("bad", [{"success": False}, {"success": True, "result": {"resources": []}}])
def test_api_failure_is_not_an_empty_success(bad):
    with (
        httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=bad))) as session,
        pytest.raises(ValueError),
    ):
        select_companies(2025, 1, 100, session=session)


def test_wrong_month_from_source_is_rejected():
    def handler(request):
        if request.url.path.endswith("package_show"):
            result = {"resources": [{"id": "official", "name": "2025", "datastore_active": True}]}
        else:
            result = {"records": [{"Anio": "2025", "Mes": "Febrero"}], "total": 1}
        return httpx.Response(200, json={"success": True, "result": result})

    with httpx.Client(transport=httpx.MockTransport(handler)) as session, pytest.raises(ValueError, match="outside"):
        select_companies(2025, 1, 1, session=session)

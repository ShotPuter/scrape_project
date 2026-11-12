import sqlite3

from scripts.res_audio_batch import select_companies


def test_batch_selection_keeps_control_and_excludes_previously_empty_results(tmp_path):
    path = tmp_path / "sample.sqlite3"
    with sqlite3.connect(path) as db:
        db.executescript("""
            CREATE TABLE companies(company_id TEXT, company_name TEXT, rut TEXT);
            CREATE TABLE res_lookup_results(company_id TEXT, status TEXT);
            INSERT INTO companies VALUES ('control','VERTIKAL GROUP SpA',NULL);
            INSERT INTO companies VALUES ('empty','AAA empty','11111111-1');
            INSERT INTO companies VALUES ('b','B company','22222222-2');
            INSERT INTO companies VALUES ('a','A company','33333333-3');
            INSERT INTO companies VALUES ('missing','No RUT',NULL);
            INSERT INTO res_lookup_results VALUES ('empty','not_found');
        """)
    chosen = select_companies(path, 3)
    assert [row["company_id"] for row in chosen] == ["control", "a", "b"]
    assert chosen == select_companies(path, 3)

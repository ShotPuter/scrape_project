from scripts.pep_camoufox_export import build_export


def test_export_keeps_excel_rows_all_declarations_and_multiple_pdf_paths():
    rows = [
        {"excel_row": 2, "data": {"NAME": "José", "EMPTY": None, "ID": "001"}},
        {"excel_row": 9, "data": {"NAME": "José", "ROLE": "Mayor"}},
    ]
    result = build_export(
        {
            "jobs": [
                {
                    "url": "https://example.com",
                    "status": "partial",
                    "source_records": rows,
                    "documents": [
                        {
                            "id": "1",
                            "publication_date": "2023-01-02",
                            "status": "downloaded",
                            "files": [
                                {"local_path": "worker-1/tab-2/pdf/1.pdf", "sha256": "a", "size_bytes": 10},
                                {"local_path": "worker-1/tab-2/pdf/1-2.pdf", "sha256": "b", "size_bytes": 20},
                            ],
                        },
                        {"id": "2", "publication_date": "2024-02-03", "status": "timeout"},
                    ],
                }
            ]
        }
    )
    assert result[0]["excel_records"] == rows
    assert len(result[0]["declarations"]) == 2
    assert [pdf["path"] for pdf in result[0]["declarations"][0]["pdfs"]] == [
        "worker-1/tab-2/pdf/1.pdf",
        "worker-1/tab-2/pdf/1-2.pdf",
    ]
    assert result[0]["declarations"][1]["pdfs"] == []
    assert result[0]["declarations"][1]["download_status"] == "timeout"

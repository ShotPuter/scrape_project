# Живые локальные проверки Camoufox / Webshare, 4 ноября 2026

Проверена версия production-загрузчика после коммита `302ead0`.
В ходе проверок production-код браузеров, очереди и восстановления не менялся.
Локальный runner расширен параметрами количества браузеров, вкладок и замен.
API-ключ взят из игнорируемого `.env`; Telegram в этих проверках не вызывался.
Проверки выполнялись настоящими браузерами через разные Webshare direct-прокси,
на фиксированной выборке `data/pep-firefox-10-20261103T132909Z/sample.json`.

| Сценарий | Ссылки | PDF | Время | Замены |
| --- | --- | --- | --- | --- |
| 2 браузера × 3 вкладки | 6/6 полностью | 21 | 106,57 с | 0 |
| 4 браузера × 3 вкладки | 10/10 полностью | 36 | 136,39 с | 0 |
| 2 × 3, намеренно закрыта одна вкладка | 9 полностью, 1 ожидаемая ошибка | 34 | 134,67 с | 1 |

Во всех трёх случаях отдельно проверены PDF-заголовок/EOF и SHA-256 каждого
сохранённого файла по путям из общего отчёта, включая файлы заменённых вкладок.
В последнем тесте одна страница намеренно закрывалась на `domcontentloaded`,
до нажатия скачивания. Установлено, что её ссылка сохраняется с ошибкой,
открывается замена в прежнем контексте и следующие задачи завершаются.
Все 10 номеров ссылок присутствуют в итоговом отчёте ровно по одному разу.
Намеренно проваленная ссылка автоматически не повторялась.

Результаты сохранены локально в игнорируемом `data/`:

- `pep-webshare-20261104T192843733332Z/` — 2 × 3;
- `pep-webshare-20261104T193050969874Z/` — 4 × 3;
- `pep-webshare-20261104T193354470042Z/` — проверка замены;
- `pep-local-live-validation-20261104.json` — сводка проверки файлов.

Повтор обычных проверок из корня проекта с настроенной средой Camoufox:

```bash
PYTHONPATH="$PWD/.camoufox-deps:$PWD" XDG_CACHE_HOME="$PWD/data/camoufox-cache" \
  .venv/bin/python -m scripts.pep_camoufox_webshare --workers 2 --tabs 3 --links 6 --max-seconds 360

PYTHONPATH="$PWD/.camoufox-deps:$PWD" XDG_CACHE_HOME="$PWD/data/camoufox-cache" \
  .venv/bin/python -m scripts.pep_camoufox_webshare --workers 4 --tabs 3 --links 10 --max-seconds 420
```

Runner возвращает ненулевой exit code при неполном или ошибочном результате.
Эти результаты подтверждают локальную работу 2–4 браузеров, но не подтверждают
работу 20 × 3 на сервере. Массовые серверные падения не воспроизведены локально;
их причина не установлена. Для проверки ресурсной причины нужны фактические
лимиты контейнера, RAM/CPU сервера, `resources` из серверного отчёта и системные
логи. Локальные успешные скачивания не заменяют эту диагностику.
## Browser rotation — 2026-11-05

Local Webshare run `pep-webshare-20261105T143849804647Z`: two real browsers,
three tabs each, four links, forced browser rotation after each claimed link.
Completed in 112.33 seconds: four completed links, 13 PDFs, two whole-browser
restarts, zero crash-related tab replacements. All four context closures were
expected. Every exported PDF path exists and all 13 SHA-256 values match.
The queue retained all four unique jobs across browser generations. The test
did not send Telegram messages. This validates lifecycle and persistence,
not multi-hour memory stability or server capacity.

## SQLite state — 2026-11-05

Local Webshare run `pep-webshare-20261105T165044288505Z`: two browsers with three
tabs each, four links, forced rotation after each claimed link. Finished in
100.97 seconds: four completed links, 13 PDFs, two browser restarts, zero
crash-related tab replacements and no timeouts. During the run no root
`report.json` or `politicians.json` was generated. All state was read back from
`state.sqlite3`; all 13 PDF hashes and paths passed validation. The separate
`scripts.pep_camoufox_db` command then produced the JSON export with four records
and valid PDF paths. No Telegram messages were sent. This is a short functional
test, not a throughput prediction for the full workbook.

## Navigation availability investigation — 2026-11-05

Two-browser Webshare run `pep-webshare-20261105T175532601159Z` reproduced current
navigation failures: both consultation links timed out in `waiting_result_html`,
zero PDFs, no claim of successful live downloading. Changing the lifecycle wait
alone did not restore availability. A direct request to the same consultation
URL also received zero bytes before its 20-second timeout, while a control GET
to api.ipify.org returned HTTP 200 in 0.24 seconds. These observations narrow the
failure to access to the target site but do not distinguish an outage, filtering,
or routing trouble. Unit tests cover parsed-table readiness, identity checks,
bounded timeout recovery, and no recovery for non-timeout failures.

## Whole-browser recovery — 2026-11-06

Run `pep-webshare-20261106T094221727816Z`: two real browsers with three tabs each,
four links. One real context was deliberately closed before it claimed work.
The worker detected `TargetClosedError`, closed the old owner, waited five
seconds and launched another context on the same assigned proxy. The queue
finished in 105.99 seconds: four completed links, 13 verified PDFs, one
whole-browser recovery, no failed links or timeouts. SQLite retained every
unique link and all SHA-256 values matched the saved files. Final live browser
count was zero after expected cleanup. Telegram was disabled for this test.
This validates recovery, not stability of 40 browsers × five tabs on the server.

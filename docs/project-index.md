# Карта проекта

Индекс актуален на 2026-11-03. Это карта кода и проверенного состояния, не
эмбеддинг-база. Личные данные, `.env`, SQLite, cookies и профили браузера в индекс
не включены. В рабочем дереве уже были незакоммиченные изменения разделения
источников и интеграции PEP; они не откатывались.

## Точки входа

| Команда / файл | Назначение |
| --- | --- |
| `main.py` → `src/cli.py` | Основной сборщик SEPREC, категории/годы, стресс-режим |
| `collect_proxies.py`, `python -m src.proxy` | Одноразовая или непрерывная проверка прокси |
| `python -m src.sites.pep` | Excel → ссылки Ley 2013 → документы → SQLite/JSON |
| `python -m src.sites.pep.benchmark` | Ограниченная выборка PEP, метрики, Webshare/direct, уведомления |
| `bash scripts/test_pep_stability.sh` | 10–20 одинаковых ссылок: persistent nodriver и native Chrome, затем ограниченный проход временных ошибок |
| `python -m src.sites.pep.stability_report` | Офлайн-сводка первого прохода и результата восстановления |
| `config.py`, `.env.example` | Настройки SEPREC/прокси; PEP в основном использует отдельные аргументы CLI |

## Что уже реализовано

### SEPREC: Боливия, коммерческие публикации

* `src/sites/seprec/client.py`: API каталогов, поиска и деталей, проверка ответа
  через Pydantic. `src/client/seprec_client.py` — совместимый старый импорт.
* `src/sites/seprec/scraper.py`: оркестрация; старый импорт
  `src/services/scraper.py` сохранён как alias.
* `src/services/scrape_pipeline.py`: ограниченная очередь и обработка документов.
* `src/models/publication.py`: модели API и CSV.
* `src/storage/file_storage.py`, `integrity.py`, `publication_csv.py`: проверка
  сохранённых PDF, атомарная запись, инкрементальный индекс.
* `src/services/pdf_generator.py`: генерация PDF из текста при отсутствии оригинала.
* `src/services/run_reporter.py`, `run_statistics.py`, `stress_monitor.py`:
  статистика и контроль длительных прогонов.

Этот источник работает через API; browser/CAPTCHA-эксперименты PEP его не меняют.

### Общая сеть и прокси

* `src/client/base_client.py`: маршрутизация запросов, повторы, переключение транспорта.
* `src/network/`: aiohttp/curl_cffi, сессии, лимиты и телеметрия.
* `src/proxy/collector.py`, `checker.py`, `probes.py`: двухступенчатая проверка.
* `src/proxy/repository.py`, `pool.py`, `health.py`: SQLite и рабочий пул.
* `src/proxy/sources/`: реестр источников, адаптеры, Webshare, платные списки, кэш.
* `src/services/seprec_proxy_probe.py`, `proxy_collector.py`, `proxy_bootstrap.py`:
  связь общей инфраструктуры с SEPREC и запуск коллектора.

PEP не использует этот пул автоматически. В его benchmark отдельно назначается
стабильный Webshare-прокси каждому воркеру. Успешный прокси не отменяет reCAPTCHA.

### PEP: Колумбия, funcionpublica.gov.co

* `workbook.py`: читает второй лист существующего Excel, сохраняет дубликаты строк,
  извлекает `ENLACE_CONSULTA_LEY_2013`; формулы не исполняет.
* `source.py`: разрешённый host/HTTPS, разбор таблицы, пагинации, современных форм
  и исторических ссылок. Неизвестная страница — ошибка, а не пустой результат.
* `discovery.py`: HTTP-поиск через curl_cffi, задержки/повторы и TLS preflight.
* `tls.py`: проверенное дополнение отсутствующего промежуточного сертификата;
  TLS не отключается.
* `documents.py`: HTTP-загрузка, проверка PDF, ограниченное чтение ZIP, хеши,
  атомарные файлы и manifests. Современные формы без браузера — `captcha_required`.
* `browser_download.py`: одна сериализованная страница; Chrome/CDP или Playwright;
  native click, обработка загрузки/отказа и предохранитель после блокировок.
* `__main__.py`: SQLite `rows/jobs/meta`, ограниченная очередь, checkpoint каждого
  документа, экспорт всех строк, возобновление с проверкой PDF.
* `benchmark.py`: фиксированная выборка, отдельные профили/каталог, блокировка
  параллельного запуска в тот же каталог, метрики и Telegram.
* `nodriver_download.py`: persistent nodriver, один browser/page/profile, native
  click, bounded timeouts и предохранитель; выбирается в benchmark и теперь
  доступен production CLI через `--browser-engine nodriver` как serial fallback.
  Webshare-ротация выбирается отдельно через `--proxy-sessions` и проверяет
  совпадение внешнего IP HTTP/browser перед запросами к источнику.
* `proxy_tunnel.py`: собственный loopback HTTPS CONNECT-переходник для Chrome;
  Webshare-аутентификация без пароля в аргументах браузера и без direct fallback.
* `browser_diagnostics.py`: общие безопасные статусы сети и классификация notice.
* `retry_queue.py`: SQLite-история попыток, восстановление после прерывания,
  фильтрация временных ошибок и постоянный лимит попыток.
* `stability_report.py`: сравнение одинаковых выборок и первого/повторного проходов.
  Одноразовые альтернативные probe-модули удалены после согласованной очистки.

Все перечисленные PEP-файлы находятся в `src/sites/pep/`.
Вызовы AWS/S3 этим пайплайном не выполняются.

## Установленные ограничения

1. Современная выдача документа требует reCAPTCHA с action — схема v3, не v2.
   Успешная загрузка — POST формы и ответ ZIP, не публичный прямой PDF URL.
2. Chrome с окном и Chrome под Xvfb успешно скачали по одному PDF в новых пробах.
   Полный существующий пайплайн также завершил одну ссылку с двумя PDF под Xvfb.
   Headless/stealth/PyPasser не доказаны как надёжная замена. Подробности и ограничения
   сравнения: [архив экспериментов](pep-experiments-history.md).
3. В проверенных существующих SQLite на момент начала исследования не было
   документов со статусом `downloaded`; прежние текстовые отчёты об успехах не
   являются подтверждением текущего состояния этих баз.
4. Исходные два падения discovery связаны с `_ensure_session()`: скрытый
   дополнительный запрос обходил pacing, разрешал redirects и повторял работу
   preflight. Этот запрос удалён: HTTP-инициализация остаётся в явном preflight,
   а browser warmup выполняется в собственном сеансе браузера.
5. Исходный `ruff check .`: пять замечаний в уже существующих изменениях
   (`src/cli.py`, compatibility aliases, `discovery.py`). В `browser_download.py`
   есть существующие mypy-ошибки типов необязательных browser/process объектов.
6. Год публикации не обязательно равен году декларации; успешная обработка ссылки
    не доказывает полноту документов за все годы службы.
7. Реальный тест 2026-11-03 на одинаковых 10 ссылках: оба движка завершили 5,
   затем получили verification notice на шестой; у nodriver 14 PDF, у native 17.
   SQLite retry восстановил три первоначальных UI-timeout после исправления
    прокрутки nodriver. Это не подтверждает надёжность unattended-сбора всей базы.
8. Найдены штатные bulk-report endpoint. Отчёт деклараций возвращает ZIP с XLSX
   (42,127 записей и ссылками на персональные консультации), но не PDF-файлы;
   запрос также требует штатную reCAPTCHA. Socrata open-data datasets содержат
   метаданные/флаги, но не документы. Входной workbook берётся локально из `docs/`.

## Документация и тесты

* `README.md`, `docs/server.md`: эксплуатация SEPREC.
* `docs/architecture.md`: границы источников.
* `docs/pep-local.md`, `pep-network-analysis.md`, `pep-benchmark.md`: основной PEP.
* `docs/pep-stability.md`: текущий двухпроходный тест native/nodriver и SQLite resume.
* `docs/pep-experiments-history.md`: архив удалённых одноразовых проб.
* `docs/pep-research.md`: ранняя историческая заметка до получения доступа к сайту;
  её утверждение о недоступности источника уже не описывает текущую проверку.
* `tests/test_pep*.py`: парсинг, документы, TLS, discovery, browser, benchmark.
* `tests/test_pep_nodriver_download.py`: повторное использование browser/page,
  сохранение PDF, 403/CAPTCHA breaker и cleanup.
* `tests/test_pep_retry_queue.py`: transient-only retries, attempt cap и восстановление прерванных jobs.
* Остальные `tests/test_*.py`: SEPREC, HTTP, CSV/PDF, lifecycle, прокси/провайдеры.

Живые сетевые эксперименты не входят в pytest и не запускаются при обычном тестировании.
После очистки устаревших проб и добавления queue/nodriver/stability-проверок:
**358 pytest-тестов проходят**; изменённые Python-файлы проходят Ruff.

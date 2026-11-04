# Тест PEP через Camoufox на сервере

Требуется Docker с Compose v2. Исходный Excel уже лежит в
`docs/DECLARACIONES_PEP.xlsx`; контейнер открывает его только для чтения.
Браузер и его модель скачиваются при сборке образа, поэтому первая сборка
потребует несколько гигабайт диска и доступа к PyPI/GitHub.

## Запуск

Из корня распакованного проекта:

```bash
cp .env.camoufox.example .env.camoufox
```

В `.env.camoufox` укажите `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`.
Без них отчёты остаются в логах и `progress.jsonl`.

```bash
docker compose --env-file .env.camoufox -f compose.camoufox.yml build
docker compose --env-file .env.camoufox -f compose.camoufox.yml up -d
docker compose --env-file .env.camoufox -f compose.camoufox.yml logs -f
```

По умолчанию берутся первые **100 уникальных ссылок** из второй вкладки Excel,
колонка `ENLACE_CONSULTA_LEY_2013`. Дубликаты не обрабатываются повторно.
HTTP-ссылки из Excel преобразуются в HTTPS. Файл Excel не скачивается и не меняется.

Параметры `.env.camoufox`:

| Параметр | Значение по умолчанию | Назначение |
| --- | --- | --- |
| `PEP_INPUT` | `./docs/DECLARACIONES_PEP.xlsx` | Путь к Excel на сервере |
| `PEP_LIMIT` | `100` | Число уникальных ссылок, `0` — все |
| `PEP_OFFSET` | `0` | Сколько уникальных ссылок пропустить |
| `PEP_TIMEOUT` | `60` | Таймаут одного действия в секундах |
| `PEP_MAX_MINUTES` | `120` | Общий лимит времени |

Например, следующая сотня: `PEP_LIMIT=100`, `PEP_OFFSET=100`.
Один браузер и один профиль на запуск, пауза 3 секунды, без прокси.
При явном отказе reCAPTCHA прогон останавливается; при ошибке навигации
статус ссылки фиксируется, затем проверяется следующая.

## Статистика и результаты

Telegram приходит **каждые 10 обработанных уникальных ссылок**, включая ошибки,
и при завершении/остановке (также если последняя группа меньше десяти).
Счётчики накопительные с начала запуска:

- ссылки: полностью скачаны / частично / ошибки / ожидают;
- декларации: скачано / не скачано из найденных;
- число сохранённых PDF и причина остановки.

Одна ссылка может содержать несколько деклараций. Если страницу открыть не
удалось, число документов на ней неизвестно: ссылка учитывается как ошибка,
а неизвестные документы не прибавляются к счётчику найденных.
Статистика дублируется в логах и `progress.jsonl`; ошибка Telegram не прерывает сбор.
Токены CAPTCHA, cookies и Telegram credentials в отчёты не записываются.

Результаты хранятся в Docker volume `pep-camoufox-data`, внутри
`/app/data/run-<UTC>/`: `report.json`, `sample.json`, `progress.jsonl`,
`pdf/`, `archives/`, `manifests/`. ZIP и PDF проверяются перед отметкой успеха.
Каждый новый запуск создаёт отдельный каталог; это тест, автоматического
возобновления всей очереди после перезапуска здесь нет.

Скопировать результаты на сервер:

```bash
mkdir -p pep-results
docker compose --env-file .env.camoufox -f compose.camoufox.yml cp pep-camoufox:/app/data/. ./pep-results/
```

Остановить с сохранением отчёта:

```bash
docker compose --env-file .env.camoufox -f compose.camoufox.yml stop
```

Для проверки чтения Excel без браузера и без отправки Telegram:

```bash
docker compose --env-file .env.camoufox -f compose.camoufox.yml run --rm pep-camoufox /input/DECLARACIONES_PEP.xlsx --limit 10 --preview
```

При неполном результате программа возвращает exit code 1; подробная причина
находится в `report.json`. Автоматический рестарт отключён.

## Проверки пакета

Compose прошёл `config --quiet`, чтение исходного Excel проверено через
`--preview`. Прошли 7 офлайн-тестов выбора ссылок, границ Telegram-отчётов,
обработки недоступного Telegram и проверки ZIP/PDF; Ruff без замечаний.
Полную локальную сборку и запуск контейнера подтвердить не удалось:
загрузки зависимостей с PyPI обрывались/зависали, в том числе через сеть хоста.
Архив содержит исходники для сборки на сервере, а не готовый Docker image.

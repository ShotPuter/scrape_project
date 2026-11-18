# Автоматический RES-пайплайн в Docker

`bulk-run` выполняет поиск CVE по датам, получает токен без оператора
(checkbox, затем локальный Whisper при необходимости), повторно использует
токен для верификации нескольких CVE и скачивает выданные PDF параллельно.
Patchright работает на виртуальном дисплее; монитор и ручное решение не нужны.
При отказе CAPTCHA запуск сохраняет очередь и завершается с ошибкой.
При суточном лимите генерация останавливается; готовые ссылки сохраняются и скачиваются.
Количество PDF на один токен зависит от ответов RES и срока действия токена.

Нужны Docker Engine и Docker Compose v2 на Linux x86-64, минимум 6 ГБ свободной
памяти и около 10 ГБ свободного диска для сборки. Whisper base, Chrome и Xvfb
входят в образ. Локальные Python, браузер и Whisper не требуются.

На новом сервере:

```bash
git clone --branch feature/chile-dequienes-res git@github.com:ShotPuter/scrape_project.git
cd scrape_project
cp .env.res.bulk.example .env.res
nano .env.res
mkdir -p data
```

Если `.env.res` уже существует, сохраните его и добавьте параметры из примера.
Заполните `WEBSHARE_API_TOKEN` и `ABUSEIPDB_API_KEY`. Пример явно включает
`PROXY_REPUTATION_MODE=blocklist_only`, как в успешной реальной проверке:
обязательные списки блокировки проверяются, индивидуальный CHECK не вызывается.
Для прямого подключения можно задать `RES_BULK_PROXY_PROVIDER=direct`.
Прокси не меняется автоматически при отказе или достижении суточной квоты.

Удобная команда для всех следующих примеров:

```bash
dc() { docker compose --env-file .env.res -f compose.res.yml -f compose.res.bulk.yml "$@"; }
dc build res
dc run --rm --user 0 --entrypoint sh res -c \
  'mkdir -p /app/data/res-bulk && chown 1000:1000 /app/data /app/data/res-bulk'
```

Проверка с ограничением 10 документов на реальном сайте:

```bash
dc run --rm res bulk-run --start 2025-07-29 --end 2025-07-29 \
  --db data/res-bulk/docker-check/tracking.sqlite3 --run-id docker-check \
  --proxy-provider webshare --limit 10
cat data/res-bulk/docker-check/bulk-report.json
find data/res-bulk/docker-check/ready-downloads -name '*.pdf' | wc -l
```

Основной запуск (по умолчанию до 100 CVE за один запуск):

```dotenv
RES_START_DATE=2025-07-29
RES_END_DATE=2025-07-29
RES_BULK_RUN_ID=res-bulk-2025-07-29
RES_BULK_LIMIT=100
RES_BULK_MAX_TOKENS=4
RES_BULK_WORKERS=4
```

```bash
dc up -d res
dc logs -f --tail=100 res
dc ps -a
```

Данные сохраняются в `data/res-bulk/<RES_BULK_RUN_ID>/`:

- `tracking.sqlite3` — найденные CVE, статусы, выданные ссылки и контрольные суммы;
- `bulk-report.json` — результат запуска, число токенов, проверенных CVE и PDF;
- `ready-downloads/pdf/` — PDF и манифесты;
- `profile/` — частный браузерный профиль.

Для остановки: `dc stop res`. Для продолжения: снова `dc up -d res`.
Завершённые дни и скачанные PDF пропускаются. `RES_BULK_LIMIT` ограничивает
число новых верификаций и HTTP-загрузок в одном запуске; повторный запуск может
обработать следующую часть очереди. Для другой выборки задайте новый `RES_BULK_RUN_ID`.
После суточного лимита повторный запуск выполняйте после сброса квоты RES.
Автоматического бесконечного перезапуска нет (`restart: "no"`).

Секреты передаются только при запуске. `.env`, PDF, SQLite и браузерные профили
исключены из build context и Git. Токены CAPTCHA в отчёты не записываются.

Проверено 18 ноября 2026 из собранного образа `chile-res:bulk`: найдено
1 271 действие за 2025-07-29, Whisper автоматически распознал аудио для поиска
и верификации. Один токен верификации использован для 10 CVE; скачаны 10 новых
PDF без ошибок (6 393 269 байт). Верификация заняла 14,27 секунды,
скачивание — 9,19 секунды. Отчёт локальной проверки:
`data/res-bulk/docker-check-20261118/bulk-report.json`.

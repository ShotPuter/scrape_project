# RES fleet: запуск на сервере

Fleet запускает до пяти браузеров по умолчанию, каждый с отдельным маршрутом.
Приоритет определяется `RES_FLEET_PROVIDERS`. Состояние очереди, бюджеты,
парковка и документы сохраняются в `RES_DATA_DIR` и переживают пересоздание контейнера.
Завершённая выборка останавливает контейнер; это ожидаемое поведение пакетного запуска.

Нужны Linux x86-64, Docker Engine с Compose v2, память для выбранного количества
браузеров и место для образа и PDF. Лимиты памяти/CPU задаются в `.env.res`;
пять браузеров проверены в реальном запуске, но требуемая память зависит от страниц.

## Конфигурация

На новом сервере скопируйте `.env.res.example` в `.env.res`. На существующем
сохраните `.env.res` и каталог результатов. Пример первого запуска с уже
проверенными Webshare и бесплатными маршрутами:

```dotenv
WEBSHARE_API_TOKEN=YOUR_TOKEN
ABUSEIPDB_API_KEY=YOUR_KEY
RES_FLEET_PROVIDERS=webshare,free
PROXY_REPUTATION_MODE=strict
RES_BROWSERS=5
RES_YEAR=2025
RES_MONTH=1
RES_LIMIT=100
RES_FLEET_RUN_ID=res-fleet-2025-01-100
RES_FLEET_ROUTE_DOCS=95
RES_FLEET_BUDGET_WEBSHARE=100
RES_FLEET_BUDGET_FREE=100
RES_FREE_PROXY_PORTS=80,443,1080,3128,8080,8000,8888,8081,8088
RES_DATA_DIR=./data
```

Бюджеты здесь ограничивают первый запуск 100 документами на провайдера за сутки;
`0` отключает дополнительную капу. При достижении лимита провайдер ждёт UTC-сброса.
Проверки репутации, блоклистов, совпадения IP и TLS остаются включены.
Указанные порты задают прикладной список, но не изменяют firewall сервера.
См. [Docker egress firewall](docker-egress-firewall.md), если он настроен на хосте.

Bright Data добавляется в список провайдеров после настройки `BRIGHTDATA_PROXY_*`
и разрешения исходящего IP сервера в аккаунте. Локальная проверка 19 ноября
получила `ip_blacklisted`; Webshare и один бесплатный маршрут скачали настоящие PDF.
Результаты проверки конкретного публичного адреса не гарантируют его доступность позднее.

Для статических платных прокси добавьте `paid` и задайте `PROXY_PAID_FILE` внутри
смонтированного `/app/data`, например `data/paid-proxies.txt`.

## Сборка

Из корня нужной версии репозитория:

```bash
export RES_IMAGE="chile-res:fleet-$(git rev-parse --short HEAD)"
docker build -f Dockerfile.res -t "$RES_IMAGE" .
```

Обычная сборка загружает зависимости, Camoufox, модель отпечатков, GeoIP и Whisper.
Если публичная загрузка браузера недоступна, можно использовать существующие
совместимые публичные assets как именованные BuildKit-контексты:

```bash
docker build -f Dockerfile.res -t "$RES_IMAGE" \
  --build-arg CAMOUFOX_ASSETS=cache \
  --build-context browser-assets=./data/camoufox-cache/camoufox/browsers \
  --build-context model-assets=./.camoufox-deps/fpgen/data .
```

Копируются только браузерные файлы и модель, без профилей, cookies, `.env` и PDF.
Готовый образ содержит код и assets; монтировать исходники при production-запуске
не требуется. `RES_IMAGE` можно сохранить в серверном `.env.res`, чтобы Compose
всегда использовал выбранную версию.

## Запуск и проверка

```bash
dc() { docker compose --env-file .env.res -f compose.res.yml -f compose.res.fleet.yml "$@"; }
dc config --quiet
mkdir -p data
dc run --rm --no-deps --user 0 --entrypoint sh res -c \
  'mkdir -p /app/data/res-benchmark && chown 1000:1000 /app/data /app/data/res-benchmark'
dc run --rm --no-deps res fleet --help
dc up -d --no-build res
dc logs -f --tail=100 res
```

Если `RES_DATA_DIR` отличается от `./data`, создайте именно этот каталог до запуска.
Образ работает от UID/GID 1000. Не запускайте одновременно две fleet-копии
с одним каталогом результатов: dispatcher и дочерние процессы держат общую блокировку.

Проверяйте `data/res-benchmark/<RES_FLEET_RUN_ID>/progress.json`, затем `summary.json`:
число активных и готовых браузеров, новые PDF, ошибки компаний и состояние маршрутов.
Файлы PDF находятся в подкаталогах `workers/w*/pdf/`; tracking хранит SHA-256.
Базы `tracking.sqlite3` и `fleet-runtime.sqlite3` находятся в `data/res-benchmark/`.

Остановка с сохранением прогресса и последующее продолжение:

```bash
dc stop -t 60 res
dc up -d --no-build res
```

Для смены образа оставьте тот же `.env.res`, run-id и каталог данных, поменяйте
`RES_IMAGE` на собранный тег и выполните `dc up -d --no-build res`.
Для отката укажите прежний тег тем же способом. Не удаляйте каталог данных.
При изменении выборки (год/месяц/limit) используйте новый `RES_FLEET_RUN_ID`.

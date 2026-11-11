# PEP: проверка стабильности native Chrome и nodriver

## Первый реальный прогон, 2026-11-03

`data/pep-stability-10-20261103/comparison.json`: одинаковая выборка подтверждена
`sample_sha256`. На первом проходе nodriver полностью завершил 2 ссылки,
скачал 6 PDF и остановился на трёх timeout на третьей ссылке; 7 ссылок остались
pending. Native CDP завершил 5 ссылок, скачал 17 PDF и получил явный отказ
reCAPTCHA на шестой; 4 ссылки остались pending. Также остался один исторический
HTTP 404. Таким образом, ни один первый проход не подтвердил стабильные 10/10.

Разбор nodriver без отправки форм (`nodriver/button-geometry.json`) выявил
конкретную ошибку адаптера: три успешные кнопки были внутри viewport высотой
973 px, а три ненажатые — на y=989–1362. `mouse_click()` не прокручивал страницу
автоматически, и POST для них не наблюдался. Адаптер v2 явно вызывает
`scroll_into_view()` и проверяет, что кнопка видима и получает указатель.
Такой сбой нельзя объявлять отказом CAPTCHA. Отдельный ручной ограниченный
проход `errors` в том же профиле восстановил все три недостающих PDF: стало
3 завершённые ссылки и 9 PDF. Успешные документы не перекачивались. Затем
возобновлён проход по оставшимся 7 `pending` ссылкам с тем же лимитом попыток.

Native после подтверждённого verification notice повторно не отправлялся.
При интерпретации восстановления nodriver нужно учитывать изменение кода
адаптера, а не приписывать улучшение только повторному запуску.

### Итог на той же выборке

| Движок | Полностью завершено | Проверенных PDF | Остались pending |
| --- | --- | --- | --- |
| nodriver | 5/10 | 14 | 4 |
| Native Chrome/CDP | 5/10 | 17 | 4 |

Оба остановлены после явного verification notice на шестой ссылке. Есть также
один HTTP 404 исторического файла; он сохранён как ошибка источника. Ни один
вариант не подтвердил стабильные 10/10, а nodriver не продемонстрировал улучшения
прохождения reCAPTCHA. Оставшиеся четыре ссылки не отправлялись после остановки.

У nodriver было три запуска: исходный `all`, ограниченный `errors` после
исправления прокрутки и `all` для оставшихся pending. Это не три попытки одной
ссылки: первые три завершённые ссылки при последнем запуске только проверены
локально. SQLite подтверждает максимум две попытки на ссылку у nodriver и одну
у native. Все 14 + 17 сохранённых документов повторно прошли проверку
manifest/hash/PDF, повреждённых файлов нет.

`comparison.json` содержит первый проход отдельно от окончательного состояния,
список запусков и ревизии адаптера. Сравнение времени всех запусков nodriver
с единственным native-запуском не является чистым benchmark скорости: в него
попадает исправление первоначального адаптерного сбоя.

Практический вывод: SQLite-очередь и восстановление ошибок работают, но не
устраняют серверный отказ reCAPTCHA. Для гарантированного unattended-сбора нужна
договорённость с источником/API либо иной разрешённый режим доступа; новые
движки сами по себе такой гарантии не дали.

## Методика

Два движка обрабатывают **одни и те же первые 10 уникальных ссылок** workbook.
Для каждого движка используется один постоянный Chrome-профиль
и одна сериализованная страница. Физический монитор и desktop не нужны: Xvfb.

## Запуск

### Только браузер, без HTTP-клиента к порталу

`--browser-only` доступен для `nodriver` и `undetected`. В этом режиме
`DiscoveryClient`/`DocumentDownloader` не создаются: bootstrap, чтение консультаций,
пагинация и скачивание выполняются в одном Chrome. Таблица читается из DOM после
ожидания `table.table tbody`; её HTML разбирается существующим `parse_results`.
Для современных деклараций нажимается штатная кнопка формы, для старых — ссылка
на файл. Сохранённые валидные PDF повторно не скачиваются.

Webshare inventory API и Telegram остаются служебными интеграциями. Внешние
CAPTCHA API не подключаются. Внешний IP проверяется **самим Chrome** через ipify,
без отдельного Python HTTP-запроса; отчёт пишет `routing: browser_verified`,
а не утверждает совпадение двух HTTP-клиентов. Browser-generated запросы самого
сайта, включая reCAPTCHA и отправку формы скачивания, никуда не исчезают.

Выбирайте новый `--output` при смене режима. Для сохранения браузерного профиля
между отчётами используется прежний `--browser-profile-root`. Пример с текущим
проверенным CA bundle и настройками первого Webshare-прокси:

```bash
xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  .venv/bin/python -u -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --browser-only --network webshare --headed --browser-engine undetected \
  --workers 1 --count 1 --delay 3 --timeout 40 --max-minutes 3 --notify-interval 30 \
  --max-job-attempts 1 --single-browser-attempt \
  --browser-renderer swiftshader --require-webgl --mouse-motion \
  --browser-language es-CO --browser-timezone America/Los_Angeles \
  --browser-profile-root data/pep-browser-profiles --browser-diagnostics \
  --page-settle-min 4 --page-settle-max 8 --page-settle-scroll \
  --ca-bundle data/pep-benchmark-50/webshare-3x1/ca-bundle.pem \
  --output data/pep-browser-only-comparison-1
```

Live-проверка 2026-11-03: `data/pep-browser-only-pilot-1/`, run `2528184bb9e1`.
С тем же профилем и egress, что в UC-пилоте 3, браузер распарсил две декларации;
первое скачивание получило 302 и `site_verification_notice`, второе не отправлялось.
0 PDF за 60 секунд. Чтение метаданных работает; исчезновение отдельного Python
HTTP-клиента не устранило отказ выдачи файла в этой пробе. Browser-only режим
не является способом отключить серверную проверку reCAPTCHA.

Офлайн-тесты запрещают создание HTTP discovery/downloader в browser-only режиме,
проверяют DOM-пагинацию, остановку после verification refusal, local reuse и
браузерную загрузку старых ссылок. Общий pacing теперь использует `RequestPacer`
без создания лишней HTTP-сессии в обоих режимах.

### Экспериментальный undetected-chromedriver

Добавлен отдельный `--browser-engine undetected`. UC запускает Chrome/ChromeDriver,
а существующий nodriver/CDP-адаптер обслуживает события и штатную кнопку скачивания.
Профиль сохраняется по proxy fingerprint; TLS-проверка и Chrome sandbox включены.
Пока допускается только `--workers 1`, без `--proxy-sessions`, чтобы не создавать
гонку загрузки/патчинга общего ChromeDriver. Требуется доступ к официальному
репозиторию ChromeDriver при первом запуске; версия выбирается по установленному Chrome.
Optional-зависимости: `requirements-browser-uc.txt` (UC GPLv3, nodriver AGPLv3).

```bash
.venv/bin/python -m pip install -r requirements-browser-uc.txt
xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  .venv/bin/python -u -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network webshare --headed --browser-engine undetected --workers 1 --count 1 \
  --delay 3 --timeout 40 --max-minutes 3 --notify-interval 30 \
  --max-job-attempts 1 --single-browser-attempt \
  --ca-bundle data/pep-benchmark-50/webshare-3x1/ca-bundle.pem \
  --output data/pep-uc-comparison-1
```

Для сравнения с другим движком нужен отдельный `--output`; исходные checkpoint
и профили не перезаписываются. После явного verification refusal прогон прекращается.
Отсутствие TLS-ошибки, наличие токена и название библиотеки сами по себе не означают,
что сервер примет reCAPTCHA. Токены, cookies и proxy credentials в отчёт не пишутся.

Live-проба 2026-11-03: UC 3.5.5, Selenium 4.52.0, Chrome 154, Xvfb, один проверенный
Webshare-прокси; landing HTTP 200, отправка штатной формы HTTP 302 с
`site_verification_notice`, **0 PDF**. Отчёт: `data/pep-uc-pilot-1/benchmark.json`.
Это одна проба, а не оценка общей вероятности успеха или измерение reCAPTCHA score.

`ddddocr` не добавлен: встроенные модели распознают буквенные CAPTCHA/слайдеры,
но не реализуют протокол reCAPTCHA v3, используемый этим источником. Прогонять
OCR по скриншоту вместо выполнения site-generated token flow здесь бессмысленно.
Постоянный профиль уже реализован; вход в Google не гарантирует score 0.9.
Подмена платформы/видеокарты на случайные значения не включается автоматически.

#### Диагностика и задержка перед кнопкой

Для nodriver и UC доступны opt-in флаги:

* `--browser-diagnostics`: локальный JSON с UA, UA Client Hints, platform,
  `navigator.webdriver`, реальными screen/window размерами и WebGL vendor/renderer.
  Сторонние fingerprint-сервисы не посещаются; cookies, текст страницы и токены
  не читаются. Отчёт `browser-environment-<profile-ref>.json` создаётся с mode 0600.
* `--page-settle-min 4 --page-settle-max 8`: случайная пауза перед штатной кнопкой;
  оба значения по умолчанию 0, допустимо `0 <= min <= max <= 15` секунд.
* `--page-settle-scroll`: небольшой скролл и возврат в исходную позицию во время
  паузы. Отдельного вызова `grecaptcha.execute` этот режим не делает.

В `events.jsonl` сохраняются выбранная длительность паузы
(`browser_settle_seconds`) и фактическое время от завершения навигации до клика
(`browser_page_age_at_click_seconds`). Таймаут загрузки не увеличивается.
Изменение параметров паузы требует нового `--output`, чтобы не смешивать режимы.

```bash
xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  .venv/bin/python -u -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network webshare --headed --browser-engine undetected --workers 1 --count 1 \
  --delay 3 --timeout 40 --max-minutes 3 --notify-interval 30 \
  --max-job-attempts 1 --single-browser-attempt --browser-diagnostics \
  --page-settle-min 4 --page-settle-max 8 --page-settle-scroll \
  --ca-bundle data/pep-benchmark-50/webshare-3x1/ca-bundle.pem \
  --output data/pep-uc-settle-comparison-1
```

Второй live-пилот: `data/pep-uc-pilot-2/`, run `2a057889e2db`, 2026-11-03.
Один проверенный Webshare egress (тот же reference, что в первом пилоте),
новый отдельный постоянный профиль, Xvfb. Пауза 4.619 секунды, возраст страницы
при клике 7.043 секунды; landing 200, POST 302, `site_verification_notice`, 0 PDF.
Диагностика: Linux UA/platform/Client Hints согласованы, `webdriver=false`,
screen 1920×1080, фактический outer window 1919×1079; WebGL недоступен,
vendor/renderer неизвестны. Отсутствие renderer не означает аппаратный рендеринг;
при неизвестном renderer новая версия диагностики пишет
`software_renderer_detected: null` (первоначальный артефакт пилота содержит false).

Точная причина отказа и score неизвестны. Наличие Xvfb не доказывает score 0.1–0.3;
Mesa сама по себе не признак software renderer. VNC/Xpra без настроенного доступа
к GPU не обеспечивают аппаратное ускорение. Узлы `/dev/dri` на этой машине есть,
но доступность WebGL в Chrome требует отдельного расследования. WebGL-строки
не подменяются, потенциально небезопасные GPU-флаги автоматически не включаются.
Мобильный/residential-прокси не тестировался: альтернативный endpoint не предоставлен.
Даже успех на другом IP после изменения нескольких условий не доказал бы, что
единственная причина — репутация Webshare; нужна серия контролируемых сравнений.

#### Явный рендеринг, локаль, движение мыши и общий профиль

Новые opt-in параметры для nodriver/UC:

| Параметр | Действие |
| --- | --- |
| `--browser-renderer swiftshader` | `--use-gl=angle --use-angle=swiftshader --enable-webgl`; реальный software renderer, не подмена строк |
| `--require-webgl` | Сохраняет диагностику и останавливает worker до обработки заданий, если WebGL-контекст недоступен |
| `--mouse-motion` | 15–25 CDP `mouseMoved` по кривой Безье, затем press/release с короткой паузой; без DOM `flash_point` |
| `--browser-language es-CO` | Chrome `--lang`, preference `intl.accept_languages`, CDP locale и одинаковый HTTP/browser `Accept-Language` |
| `--browser-timezone America/Los_Angeles` | CDP timezone override; оператор выбирает по прокси, не по стране сайта |
| `--browser-profile-root data/pep-browser-profiles` | Профили `<root>/<engine>/<proxy-ref>` сохраняются независимо от `--output` |

Sandbox и TLS verification остаются включены. `--enable-unsafe-swiftshader`,
`--disable-gpu`, WebGL vendor spoofing и автоматическая замена прокси не добавлены.
`libgl1-mesa-dri` на текущем хосте уже установлен; системные пакеты не менялись.

Корень общих профилей блокируется через flock между всеми benchmark-прогонами,
включая разные `--output`. Сохраняется binding конфигурации engine/render/locale
и хеша проверенного egress. При изменении IP или конфигурации скрипт отказывается
использовать этот профиль. Credentials и полный IP в binding не пишутся.
Для shared root требуется Webshare с проверкой маршрута. Ручной Chrome с тем же
профилем одновременно запускать нельзя. Профиль нельзя переносить между живыми
процессами, пока они его используют.

HTTP TLS bootstrap теперь тоже идёт через первый назначенный прокси, а не напрямую.
HTTP и browser сохраняют независимые cookie jars; настройки локали их не объединяют.
Сторонние score-demo, произвольные сайты для «прогрева», импорт Google cookies и
платные solver API автоматически не используются.

Пример **одного** ограниченного прогона (timezone ниже подходит к заявленному
Webshare городу Los Angeles первого прокси текущего inventory; для другого
прокси её нужно явно выбрать заново):

```bash
xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  .venv/bin/python -u -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network webshare --headed --browser-engine undetected --workers 1 --count 1 \
  --delay 3 --timeout 40 --max-minutes 3 --notify-interval 30 \
  --max-job-attempts 1 --single-browser-attempt \
  --browser-renderer swiftshader --require-webgl --mouse-motion \
  --browser-language es-CO --browser-timezone America/Los_Angeles \
  --browser-profile-root data/pep-browser-profiles --browser-diagnostics \
  --page-settle-min 4 --page-settle-max 8 --page-settle-scroll \
  --ca-bundle data/pep-benchmark-50/webshare-3x1/ca-bundle.pem \
  --output data/pep-uc-configured-comparison-1
```

Live-пилот 3: `data/pep-uc-pilot-3/`, run `b2b59a17abb4`, 2026-11-03.
WebGL **доступен**: `Google Inc. (Google)`, ANGLE Vulkan/SwiftShader Device
(Subzero), без небезопасных fallback-флагов. Languages `es-CO, es`, locale
`es-CO`, timezone `America/Los_Angeles`, Linux UA/platform согласованы.
Перед кликом отправлено 22 mouseMoved, выбранная пауза 5.835 секунды, возраст
страницы при клике 9.303 секунды. Landing 200; первая отправка 302 с
`site_verification_notice`, **0 PDF за 46 секунд**. Вторая декларация не отправлялась.
Общий профиль создан новым: его долговременный reuse ещё не проверялся на живом
сайте; повторное использование между отчётами и отказ при смене egress покрыты
офлайн-тестами. Несколько параметров изменены вместе, поэтому эта проба проверяет
работоспособность конфигурации, а не причинное влияние каждого параметра.

Нет оснований обещать score по отсутствию/наличию WebGL или по результату чужого
демо. Токен стороннего API также не имеет гарантии принятия целевым сервером;
без ключа, бюджета и подходящего прокси такая интеграция не запускалась.

### Параллельный Webshare-пилот: 3 браузера, по одной вкладке

Первый безопасный шаг масштабирования — три одновременных nodriver-браузера,
каждый со своим отдельным Webshare proxy endpoint; для каждого HTTP/browser
маршрута проверяется совпадение и уникальность внешнего IP. В каждом Chrome
пока одна рабочая вкладка, а глобальный pacing остаётся общим. Увеличение до
трёх вкладок на браузер — следующий отдельный этап, только после оценки этого
пилота. Нужен `WEBSHARE_API_TOKEN` в окружении; само значение не выводить.

```bash
xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  .venv/bin/python -u -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network webshare --headed --browser-engine nodriver --workers 3 --count 50 \
  --delay 3 --timeout 40 --max-minutes 30 --pass-kind all \
  --max-job-attempts 2 --single-browser-attempt \
  --output data/pep-benchmark-50/webshare-3x1
```

Это использует штатную reCAPTCHA. При явном сообщении об отказе verification
останавливается весь прогон; автоматически открывать новую сессию с другим IP
или менять размер окна в ответ на отказ нельзя. Увеличивать timeout не стали.
Текущий `--workers 3` означает **3 браузера × 1 рабочая вкладка**, не 3×3.
Сначала оцените `benchmark.json`, `events.jsonl` и `proxy_sessions`; не повышайте
параллелизм при таймаутах или verification refusal.

Нужны Google Chrome, Xvfb/xvfb-run, xauth, flock, GNU timeout и non-root пользователь.
Основные зависимости — `requirements.txt`, дополнительный nodriver —
`requirements-browser.txt` (AGPLv3; учесть условия перед распространением/production).

```bash
.venv/bin/python -m pip install -r requirements-browser.txt
bash scripts/test_pep_stability.sh
```

Для 20 ссылок:

```bash
PEP_COUNT=20 bash scripts/test_pep_stability.sh
```

Можно передать существующий Excel и отдельный каталог группы:

```bash
PEP_CA_BUNDLE="$PWD/data/pep/ca-bundle.pem" \
  bash scripts/test_pep_stability.sh docs/DECLARACIONES_PEP.xlsx data/my-stability-test
```

CA bundle должен быть ранее проверенным. Без явного bundle работает штатный TLS
bootstrap. TLS и sandbox не отключаются. Внешний desktop DISPLAY/Wayland не
передаётся браузерам. Встроенные Telegram-уведомления этого теста выключены;
вывод в консоль и файлы остаётся.

Порядок — nodriver, затем native CDP. На движок: один воркер, старт запросов не
чаще раза в три секунды, operation timeout 40 секунд, worker budget 15 минут.
Внешний GNU timeout по умолчанию ограничивает процесс 20 минутами, затем TERM
и через 15 секунд KILL. `PEP_MAX_MINUTES` и `PEP_HARD_TIMEOUT` позволяют менять
бюджеты; при увеличении worker budget нужно согласованно увеличить hard timeout.
Не обновляйте исходники или virtualenv во время прогона.

## SQLite и два прохода

`benchmark.py` хранит исходные строки и уникальные jobs в SQLite. Новая таблица
`benchmark_attempts` хранит каждую попытку, run_id, время начала/конца и результат.

* `pending`: ссылка ещё не проверялась.
* `processing`: начата попытка; после аварийного завершения восстанавливается
  как `interrupted`, а не исчезает из очереди.
* `completed`: discovery закончен и все найденные PDF сохранены/проверены.
* `partial`: известные документы ещё не все получены.
* `discovery_failed_*`, `worker_error`, `interrupted`: отдельные причины незавершённости.

Первый проход обрабатывает фиксированную выборку. После него допускается **один
проход только по временным ошибкам**, не по всем ссылкам заново. Перед вторым
проходом — пауза 30 секунд (`PEP_RETRY_DELAY_SECONDS`). Максимум две job-попытки
на ссылку хранится в БД и не сбрасывается при перезапуске процесса.

Автоматически повторяются сетевые ошибки, таймауты, прерывания и выбранные 5xx/429,
если сессия не была остановлена предохранителем. Не повторяются явный отказ
reCAPTCHA, 403, TLS/schema ошибки, неизвестный формат файла и исторические 404.
404 остаётся незавершённым документом, не превращается в успех или пустой результат.

После browser quarantine, startup failure или TIME_LIMIT автоматического
переоткрытия сеанса/смены IP нет. Будущую ручную попытку после устранения причины
нужно рассматривать отдельно, не обходить ограничение бесконечным циклом.

В этом сравнении отключён внутренний немедленный повтор 302:
`--single-browser-attempt`. Иначе native CDP и nodriver имели бы разный бюджет
попыток. Успешные файлы повторно не скачиваются, но проверяются по manifest/hash.
Число новых скачиваний за run отделено от reused файлов.

## Где смотреть

```text
data/pep-stability-<UTC>-<pid>/
  comparison.json                 сводка двух движков
  nodriver/                       отдельный output и профиль
  native/
    state.sqlite3                 rows/jobs/meta/selection/attempts
    benchmark.json                последние метрики
    run-<id>.json                 неизменяемый финальный отчёт каждого прохода
    events.jsonl                  этапы, ошибки, HTTP, время и причины остановки
    console-all-*.log             консоль первого прохода
    console-errors-*.log          консоль прохода ошибок, если он был нужен
    xvfb-*.log                    диагностика display
    profiles/direct/              один persistent profile
    records.json                  приватные исходные строки/документы
    pdf/, archives/, manifests/
```

`sample_sha256` подтверждает одинаковую выборку, а `browser_engine` фиксируется
в SQLite: движок нельзя незаметно сменить в том же output. Групповой и отдельные
benchmark locks защищают от параллельного использования одного каталога.

Офлайн пересобрать сводку, не обращаясь к сайту:

```bash
.venv/bin/python -m src.sites.pep.stability_report data/my-stability-test
```

Сравнивать нужно **первый проход и итог после восстановления**, а не только
конечный процент. `first_pass_completed_links`, `final_completed_links`,
`additional_completed_after_first_pass`, количество попыток, причины отказов и
pending — основные поля `comparison.json`. Downloaded/discovered не показывает
полноту там, где discovery не удалось.

Отдельный ручной проход временных ошибок:

```bash
xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  .venv/bin/python -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network direct --headed --browser-engine nodriver --workers 1 --count 10 \
  --delay 3 --timeout 40 --notify-interval 30 --max-minutes 15 \
  --pass-kind errors --max-job-attempts 2 --single-browser-attempt \
  --output data/my-stability-test/nodriver
```

Это небольшой упорядоченный эксперимент, не статистическая гарантия работы на
всей базе или с другого IP. Кэшированный повтор завершённого набора не является
вторым независимым подтверждением прохождения CAPTCHA.

## Ограниченная ротация Webshare + nodriver

Отдельный, явно выбираемый эксперимент; обычный direct-тест выше не меняется.
Нужен `WEBSHARE_API_TOKEN` в окружении или `.env`. Начальная проверка — максимум
два последовательных браузерных сеанса и две новые ссылки на сеанс:

```bash
env -u DISPLAY -u WAYLAND_DISPLAY -u XDG_RUNTIME_DIR -u DBUS_SESSION_BUS_ADDRESS \
  XDG_SESSION_TYPE=x11 TELEGRAM_BOT_TOKEN= TELEGRAM_CHAT_ID= \
  xvfb-run -a -s '-screen 0 1920x1080x24 -nolisten tcp' \
  timeout --signal=TERM --kill-after=15s 10m \
  .venv/bin/python -u -m src.sites.pep.benchmark docs/DECLARACIONES_PEP.xlsx \
  --network webshare --headed --browser-engine nodriver --workers 1 \
  --proxy-sessions 2 --session-links 2 --count 4 \
  --delay 3 --timeout 30 --notify-interval 30 --max-minutes 8 \
  --max-job-attempts 2 --single-browser-attempt \
  --ca-bundle data/pep/ca-bundle.pem --output data/my-webshare-nodriver-test
```

`--proxy-sessions` ограничен 1–5, `--session-links` — 1–20. Провайдерский inventory
ограничивается числом запрошенных сеансов; одинаковые proxy hosts исключаются.
На прокси выделяется собственный постоянный профиль. Предыдущий Chrome и его
локальный переходник закрываются до запуска следующего. Смена IP не выполняется
во время CAPTCHA/скачивания. Явный refusal останавливает текущий сеанс; следующий
может брать только ещё не взятые ссылки очереди, а не повторять отказанную ссылку.
Автоматического второго прохода в этом режиме нет.

До запросов к PEP HTTP-клиент и браузер проверяют внешний IP через HTTPS ipify.
Несовпадение или сбой проверки не допускает сеанс к источнику. `proxy_sessions`
в `benchmark.json` содержит номер сеанса, хеш прокси/внешнего IP, состояние
проверки маршрута и количество взятых jobs; адреса и учётные данные не нужны
для отчёта. Одинаковые `egress_ref` означали бы, что разные endpoints не дали
смены внешнего IP, — это нельзя считать подтверждённой ротацией.

Chrome получает только адрес loopback CONNECT-переходника. Пароль Webshare
добавляется переходником только к CONNECT на провайдерский proxy endpoint,
не отправляется origin-серверу и не передаётся аргументом Chrome. Переходник
поддерживает только HTTPS CONNECT на порт 443; обычный HTTP отклоняется.
QUIC и непроксированный WebRTC UDP отключены, proxy bypass исключён. При 407,
обрыве или недоступности upstream возвращается безопасный 502, без direct fallback.
HTTP discovery/загрузка используют тот же upstream через штатную proxy-настройку.
Прямой preflight к PEP в этом режиме не выполняется; проверка TLS остаётся включена.

Локальные тесты проверяют CONNECT-аутентификацию, отказ upstream, закрытие
соединений, отсутствие пароля в launch args, несовпадение IP, порядок сеансов,
ограничение выборки и отсутствие повтора отказанной ссылки. Вместе с остальными
тестами: 367 passed; Ruff изменённых файлов проходит.

### Реальный результат Webshare, 2026-11-03

Run `b2163709b638`, каталог `data/pep-webshare-nodriver-2x2-20261103`, длительность
208 секунд. Это новый output, без переиспользования PDF из direct-сравнения.

| Сеанс | Проверка HTTP/browser IP | Взято ссылок | Найдено документов | PDF |
| --- | --- | --- | --- | --- |
| Прокси 1 | Совпадение подтверждено | 2 | 3 | 0 |
| Прокси 2 | Совпадение подтверждено | 1 | 6 | 0 |

Хеши внешнего IP различаются: смена egress действительно произошла. HTTP discovery
успешен для трёх ссылок, однако Chrome на обоих прокси получает timeout при
`browser_session_initialize` — открытии стартовой страницы PEP. Всего шесть таких
таймаутов; ещё три документа не отправлялись из-за browser breaker. Четвёртая
ссылка осталась pending. В SQLite три job-попытки, полностью завершённых ссылок нет.

До кнопок загрузки и проверки reCAPTCHA этот прогон не дошёл. Явный отказ CAPTCHA
не зарегистрирован, но и принятие токена не проверено. По этим данным нельзя
делать вывод, что Webshare блокируется reCAPTCHA или что ротация помогает ей пройти.
Точная причина browser navigation timeout пока не установлена; нужны отдельные
диагностики навигации/TLS/подресурсов через прокси. Текущую интеграцию нельзя
считать рабочим способом скачивания. Повторный live-прогон автоматически не запущен.

Примечание безопасности: в ходе разработки один pytest failure вывел действующий
API-ключ в диагностике mock. Тест исправлен на явно заданное тестовое значение;
ключ рекомендовано перевыпустить. Его значение в отчёт не переносится.

## Очистка

По согласованию удалены старые одноразовые solver/stealth/Selenium/Patchright
скрипты и их тесты. Наблюдения сведены в [архив](pep-experiments-history.md).
Удалены 10 профилей отброшенных вариантов, около 203 MiB; JSON-отчёты, SQLite,
Excel и PDF сохранены. Успешные старые native/nodriver профили и основные рабочие
каталоги оставлены. Manifest очистки — `data/pep-cleanup-*.json`.

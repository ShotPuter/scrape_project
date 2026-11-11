# Архив одноразовых PEP-проб — 2026-11-02

Устаревшие пробные скрипты и их тесты удалены по согласованию с пользователем
2026-11-03. Сохраняются исходные JSON-отчёты, SQLite и загруженные документы
под игнорируемым `data/`; удаляются только отдельные неиспользуемые browser profiles.
Рабочие `data/pep`, `pep-server`, benchmark-базы и их профили не очищаются.
Активный двухпроходный тест: [pep-stability.md](pep-stability.md).

## Проверенные одиночные сценарии

Это один документ с первой ссылки workbook, новые отдельные профили, прямое
соединение, по одной отправке в опыте. Результат не означает стабильность на
полной базе, а порядок проб и разные launch flags не позволяют доказать причину
изменения оценки Google.

| Вариант | Наблюдение |
| --- | --- |
| Lightpanda | TLS PeerFailedVerification, HTTP не получен |
| Native Chrome headless | Страницы 200, загрузка 302 и явный отказ reCAPTCHA |
| Native Chrome с окном, включая Xvfb | Валидный PDF 102925 байт, HTTP 200 |
| Playwright с окном | 302 и явный отказ reCAPTCHA |
| playwright-stealth 2.0.3 headless / окно | 403 на главной / 302 и отказ reCAPTCHA |
| CloakBrowser 0.7.0 | Не проверялся на сайте: нет обязательного license key |
| PyPasser 1.1.0 HTTP-only | Токен получен, но целевой сервер вернул 302 и отказ reCAPTCHA |
| PyPasser HTTP token + свежий browser anchor, headless / Xvfb | В обоих случаях 302 и отказ reCAPTCHA |
| PyPasser Page Mode + Xvfb | Не прочитал токен: нет `#g-recaptcha-response`; нативная кнопка всё же скачала PDF |
| Selenium 4.52.0 + ChromeDriver | 302 и явный отказ reCAPTCHA |
| nodriver 0.50.99 + Xvfb | Валидный PDF 102925 байт, HTTP 200 |
| Patchright 1.64.3 | 302, просмотр notice не удался; точная причина не подтверждена |

В исходном пробном nodriver-адаптере `Tab.get()` создавал новый CDP session и
терял включённые Network/Page domains; исправлено на `cdp.page.navigate` в том
же session. Patchright по умолчанию читал `grecaptcha` в изолированном мире;
проверка в main world устранила ложный timeout, но сервер всё равно вернул 302.
Эти адаптерные ошибки не должны интерпретироваться как отказ CAPTCHA.

Ранее полный основной пайплайн под Xvfb обработал одну консультацию с двумя PDF
(102925 и 102404 байта); первый документ потребовал второй попытки после 302.
Напротив, benchmark `03a16fef7f99` получил явный отказ на первой ссылке и оставил
49 ожидающих. Это важное ограничение предшествующих одиночных успехов.

## Новая UC-проба — 2026-11-03

`data/pep-uc-pilot-1/`, run `14c2f60ad37d`: undetected-chromedriver 3.5.5,
Selenium 4.52.0, Chrome 154.0.8037.97, Xvfb, один Webshare-прокси.
Совпадение HTTP/browser egress подтверждено; landing вернул 200, кнопка была
видима и доступна для клика. Первая отправка вернула 302 и явный отказ
`site_verification_notice`; вторая обнаруженная декларация не отправлялась.
За 47 секунд скачано 0 PDF. TLS verification и sandbox не отключались.
Результат не доказывает причину отказа и не раскрывает оценку Google.
В отличие от архивных скриптов этот движок доступен в актуальном benchmark:
`--browser-engine undetected --workers 1 --headed`.

### UC + пауза/скролл и диагностика — 2026-11-03

`data/pep-uc-pilot-2/`, run `2a057889e2db`: тот же Webshare egress reference,
одна ссылка, свежий отдельный профиль, Xvfb, timeout 40 секунд. Выбрана пауза
4.619 секунды со скроллом/возвратом; клик через 7.043 секунды после навигации.
Landing 200, первая отправка 302 с `site_verification_notice`, повторов нет,
0 PDF за 41 секунду. Локальная диагностика зафиксировала согласованный Linux
UA/platform, `webdriver=false`, screen 1920×1080, **WebGL недоступен**.
Никаких выводов о точном renderer, score или единственной причине отказа из
этой пробы сделать нельзя. Другая сеть, Xpra/VNC и WebGL spoofing не проверялись.

### UC-пилот 3 с программным WebGL — 2026-11-03

`data/pep-uc-pilot-3/`, run `b2b59a17abb4`: один Webshare-прокси, timeout 40,
новый профиль в `data/pep-browser-profiles/undetected/<proxy-ref>`, Xvfb.
WebGL заработал с `--use-gl=angle --use-angle=swiftshader --enable-webgl`:
vendor `Google Inc. (Google)`, renderer ANGLE Vulkan/SwiftShader Device (Subzero).
Небезопасный SwiftShader fallback и отключение sandbox/TLS не использовались.
Локаль HTTP/Chrome синхронизирована на `es-CO`, timezone `America/Los_Angeles`
выбрана по заявленному Webshare городу первого прокси (Los Angeles, US), не по
стране сайта. 22 события mouseMoved, пауза 5.835 секунды, клик через 9.303 секунды.
Landing 200; POST 302, `site_verification_notice`, 0 PDF за 46 секунд.
После первого отказа автоматические отправки прекращены. Гипотеза «WebGL +
движение + локаль достаточно для снятия отказа» этой пробой не подтвердилась.
Другая сеть, накопленная история профиля и внешние solver API не проверялись.

### Browser-only, без Python HTTP к порталу — 2026-11-03

`data/pep-browser-only-pilot-1/`, run `2528184bb9e1`: `--browser-only`, UC,
одна ссылка, тот же общий профиль и egress reference, что в пилоте 3.
Python `DiscoveryClient`/`DocumentDownloader` не создавались. Chrome выполнил
preflight, прочитал таблицу и распарсил две декларации. Первый штатный клик
вернул 302 и `site_verification_notice`; второй документ не отправлялся.
0 PDF за 60 секунд; выбранная пауза 4.063 секунды, 15 mouseMoved, клик через
24.112 секунды после навигации. Метаданные доступны, отказ при выдаче файла
сохраняется без независимой HTTP-сессии портала.

## Артефакты

* `data/pep-probe-*/probe.json`: первичные одноразовые HTTP/browser-пробы.
* `data/pep-probe-xvfb-pipeline/`: прежний полный запуск одной консультации.
* `data/pep-browser-compare-selenium-nodriver-1/`: первоначальные четыре драйверных пробы.
  Сводка восстановлена офлайн после сбоя оболочки при обновлении скрипта во время работы.
* `data/pep-browser-nodriver-cdp-session-fixed/probe.json`: исправленный успешный nodriver.
* `data/pep-browser-patchright-main-world-fixed/probe.json`: исправленный Patchright, HTTP 302.

Из этих каталогов не следует восстанавливать старые команды: актуальный рабочий
код теперь сосредоточен в `browser_download.py`, `nodriver_download.py`,
`benchmark.py`, `retry_queue.py` и `scripts/test_pep_stability.sh`.
Произвольные личные HTML-страницы, токены и cookies в документацию не переносились.

## Зависимости и лицензии

PyPasser 1.1.0 — MIT (release 2026-10-22), Selenium/Patchright — Apache-2.0,
nodriver — AGPLv3. Для PyPasser даже HTTP-only импорт требовал NumPy из vision
extra; `is_solved=True` означал получение токена, не его принятие целевым сайтом.
`solverecaptchas` 0.1.6 не запускался: он предназначен для v2, а сайт использует
v3 с action. Неподходящие/неуспешные зависимости удалены из optional requirements,
но пакеты не удаляются из существующего virtualenv посреди работающего теста.

"""Read-only statistics from scraper snapshots and retained error logs."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from src.proxy.health import pool_health_path


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def statistics(directory: Path, *, errors: bool = False) -> str:
    """Read atomic progress snapshots, or aggregate retained rotated error logs."""
    report = read_json(directory / "scraper-status.json")
    if not report:
        report = read_json(directory / "stress-report.json")
    if errors:
        statuses: Counter = Counter()
        proxies: Counter = Counter()
        failures = 0
        for path in directory.glob("scraper.log*"):
            try:
                with path.open(encoding="utf-8", errors="replace") as stream:
                    for line in stream:
                        if "http_error " not in line:
                            continue
                        try:
                            event = json.loads(line.split("http_error ", 1)[1])
                        except ValueError:
                            continue
                        failures += 1
                        statuses[str(event.get("http_status") or event.get("error_class", "unknown"))] += 1
                        proxies[event.get("proxy_address", event.get("proxy_id", "unknown"))] += 1
            except OSError:
                continue
        return (
            f"Ошибки в сохранённых scraper.log*: {failures} попыток (включая повторы).\n"
            f"По типам: {dict(statuses)}\nПрокси с ошибками (топ 10): {dict(proxies.most_common(10))}"
        )
    if not report:
        return "Статистика пока отсутствует. Запустите скрапер; проверьте каталог данных бота."
    metrics = report.get("http_metrics", {})
    health = read_json(pool_health_path(directory / "proxies.db"))
    return (
        f"Статус: {report.get('status', 'unknown')}; запуск: {report.get('run_id', '—')}\n"
        f"Обновлено: {report.get('updated_at', report.get('finished_at', '—'))}\n"
        f"Сохранено/проверено: {report.get('items_scraped', report.get('documents_checked', 0))}; "
        f"пропущено: {report.get('items_skipped', 0)}; ошибок документов: {report.get('items_failed', 0)}\n"
        f"Осталось в обнаруженной очереди: {report.get('remaining_discovered', 'неизвестно')}; "
        f"всего по текущему фильтру API: {report.get('total_available', 'неизвестно')}\n"
        f"HTTP: попыток {metrics.get('attempts', 0)}, ошибок {metrics.get('failures', 0)}, "
        f"412: {metrics.get('http_412', 0)}, 429: {metrics.get('http_429', 0)}\n"
        f"Webshare: {health.get('webshare_usable', 'неизвестно')} "
        f"(снимок {health.get('updated_at', '—')}, {health.get('status', '—')}).\n"
        " Очередь не включает ещё не просмотренные страницы."
    )

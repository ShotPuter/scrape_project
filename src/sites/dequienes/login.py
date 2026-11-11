"""Open a dedicated persistent Camoufox profile for manual DeQuienes sign-in."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from src.sites.dequienes.companies import exclusive_profile


async def run(args) -> None:
    from camoufox.async_api import AsyncCamoufox

    args.profile.mkdir(parents=True, exist_ok=True, mode=0o700)
    with exclusive_profile(args.profile):
        async with AsyncCamoufox(
            headless=False, os="linux", locale="es-CL", persistent_context=True, user_data_dir=str(args.profile.resolve())
        ) as context:
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto("https://dequienes.cl/", wait_until="domcontentloaded", timeout=60000)
            print("В открытом браузере нажми Entra con Google и войди самостоятельно.", flush=True)
            print("Дождись возврата на DeQuienes. Можно проверить поиск вручную.", flush=True)
            print("Пароли, cookies и токены в терминал вводить не нужно.", flush=True)
            await asyncio.to_thread(input, "После завершения нажми Enter здесь для закрытия браузера: ")
    print(f"Браузер закрыт; профиль сохранён: {args.profile.resolve()}", flush=True)
    print("Используй тот же --profile при следующем запуске. Авторизация сайтом может истечь.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=Path("data/dequienes-account-profile"))
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except (KeyboardInterrupt, EOFError):
        raise SystemExit(130) from None
    except Exception as exc:
        # Browser exceptions can contain authentication URLs; don't expose their details.
        print(
            f"Не удалось открыть/закрыть Camoufox ({type(exc).__name__}). "
            "Проверь доступность рабочего стола и что профиль не открыт другим процессом.",
            flush=True,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

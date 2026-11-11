"""Один GET через curl_cffi с проверкой TLS; запускайте через Run/Debug в PyCharm."""

from pathlib import Path

from curl_cffi import requests

ROOT = Path(__file__).resolve().parent
URL = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/index"
CA_BUNDLE = ROOT / "data" / "pep" / "ca-bundle.pem"
OUTPUT = ROOT / "data" / "pep-curl-response.html"


def main() -> None:
    if not CA_BUNDLE.is_file():
        raise FileNotFoundError(f"Не найден CA bundle: {CA_BUNDLE}")

    with requests.Session(verify=str(CA_BUNDLE), trust_env=False) as session:
        response = session.get(
            URL,
            timeout=30,
            allow_redirects=False,
        )

        # Breakpoint здесь: смотрите response.status_code, headers, text, content.
        html = response.text
        print("URL:", response.url)
        print("HTTP:", response.status_code)
        print("Content-Type:", response.headers.get("Content-Type"))
        print("Location:", response.headers.get("Location"))
        print("Размер:", len(response.content), "байт")

        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_bytes(response.content)
        print("Ответ сохранён:", OUTPUT)
        print("\nНачало ответа:\n", html[:1000])


if __name__ == "__main__":
    main()

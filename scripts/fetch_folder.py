import pathlib
import requests


def main() -> None:
    url = "https://www2.buzzerbeater.com/community/forum/read.aspx?folder=2"
    headers = {"User-Agent": "bb-bugs-fetcher/0.1 (+polite; contact=local)"}
    resp = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
    resp.raise_for_status()
    path = pathlib.Path("data/folder2.html")
    path.write_text(resp.text, encoding=resp.encoding or "utf-8")
    print("status", resp.status_code)
    print("final_url", resp.url)
    print("bytes", path.stat().st_size)


if __name__ == "__main__":
    main()

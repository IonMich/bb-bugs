
from dataclasses import dataclass

from bb_bugs.fetch.session import PoliteSession
from bb_bugs.parse.thread_list import parse_thread_list


@dataclass
class FolderPage:
    threads: list[dict]
    pagination_context: dict
    raw_html: str


def fetch_folder_page(session: PoliteSession, url: str) -> FolderPage:
    resp = session.get(url)
    resp.encoding = resp.apparent_encoding
    threads, pagination_context = parse_thread_list(resp.text, resp.url)
    return FolderPage(threads=threads, pagination_context=pagination_context, raw_html=resp.text)


def fetch_folder_page_postback(session: PoliteSession, url: str, data: dict) -> FolderPage:
    resp = session.post(url, data=data)
    resp.encoding = resp.apparent_encoding
    threads, pagination_context = parse_thread_list(resp.text, resp.url)
    return FolderPage(threads=threads, pagination_context=pagination_context, raw_html=resp.text)

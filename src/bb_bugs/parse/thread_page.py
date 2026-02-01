
import re
from bs4 import BeautifulSoup


def parse_posts(html: str) -> list[dict]:
    """
    Return list of posts with keys: post_id, author, posted_at, body_html, body_text.
    """
    soup = BeautifulSoup(html, "lxml")
    posts: list[dict] = []

    for box in soup.select("div#messagebox"):
        header = box.find("div", class_="boxheader") or box
        author = None
        author_link = header.find("a", href=re.compile(r"/community/forum/read\\.aspx\\?teamid="))
        if author_link:
            author = author_link.get_text(strip=True)
        else:
            first_link = header.find("a")
            if first_link:
                author = first_link.get_text(strip=True)

        post_id = None
        post_link = None
        for link in header.find_all("a", href=re.compile(r"read\\.aspx\\?thread=")):
            text = link.get_text(strip=True)
            if re.match(r"\\d+\\.\\d+$", text):
                post_link = link
                post_id = text
                break

        posted_at = None
        header_text = None
        if header:
            header_text = header.get_text(" ", strip=True)
            strings = list(header.stripped_strings)
            for idx, s in enumerate(strings):
                if s.startswith("Date"):
                    cleaned = s.replace("Date:", "").strip()
                    if cleaned:
                        posted_at = cleaned
                        break
                    if idx + 1 < len(strings):
                        posted_at = strings[idx + 1].strip()
                        break

        if not post_id and header_text:
            match = re.search(r"\b\d+\.\d+\b", header_text)
            if match:
                post_id = match.group(0)

        right_col = box.find("div", id="rightColumn")
        body_container = None
        if right_col:
            body_container = right_col.find("div") or right_col

        body_html = None
        body_text = None
        if body_container:
            body_html = body_container.decode_contents()
            body_text = body_container.get_text(" ", strip=True)

        posts.append(
            {
                "post_id": post_id,
                "author": author,
                "posted_at": posted_at,
                "body_html": body_html,
                "body_text": body_text,
            }
        )

    return posts

import argparse
import json
import re
import sqlite3
import subprocess
import time
from pathlib import Path
from textwrap import shorten

PROMPT_TEMPLATE = """You are a bug-triage assistant. Analyze the thread content and output JSON ONLY.

Task:
- Summarize the bug report concisely.
- Guess status: one of [open, resolved, duplicate, not_a_bug, feature_request, unclear].
- Provide confidence: low/medium/high.
- Provide evidence: short quotes or paraphrases with post ids.
- Suggest up to 3 duplicate candidates by thread_id if obvious from content (else empty).

Rules:
- Use ONLY the provided thread content.
- Do NOT browse, search, or reference any external sources.
- Output a single JSON object and nothing else.
- No prose, no code fences.

Output JSON schema:
{{
  "thread_id": "...",
  "summary": "...",
  "status_guess": "...",
  "confidence": "...",
  "evidence": ["..."],
  "duplicate_candidates": ["..."]
}}

Thread:
{thread_blob}
"""


def load_thread(conn, thread_id: str, max_posts: int = 10) -> dict:
    thread = conn.execute(
        "SELECT thread_id, title FROM threads WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()
    if not thread:
        raise RuntimeError(f"Thread {thread_id} not found")
    rows = conn.execute(
        """
        SELECT post_id, author, posted_at, body_text
        FROM posts
        WHERE thread_id = ?
        ORDER BY post_id
        LIMIT ?
        """,
        (thread_id, max_posts),
    ).fetchall()
    title = thread["title"]
    posts = []
    for r in rows:
        body = r["body_text"] or ""
        body = shorten(body, width=2000, placeholder=" …")
        posts.append(
            {
                "post_id": r["post_id"],
                "author": r["author"],
                "posted_at": r["posted_at"],
                "body": body,
            }
        )
    return {"thread_id": thread_id, "title": title, "posts": posts}


def build_prompt(thread: dict) -> str:
    lines = [f"thread_id: {thread['thread_id']}", f"title: {thread['title']}"]
    for post in thread["posts"]:
        lines.append(
            f"post {post['post_id']} by {post['author']} at {post['posted_at']}: {post['body']}"
        )
    blob = "\n".join(lines)
    return PROMPT_TEMPLATE.format(thread_blob=blob)


def run_gemini(prompt: str, *, timeout_s: int = 120, retries: int = 2) -> str:
    cmd = ["bash", "-lc", "scripts/gemini_run.sh"]
    attempt = 0
    while True:
        try:
            attempt_timeout = timeout_s * (2 ** attempt)
            result = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=attempt_timeout,
            )
        except subprocess.TimeoutExpired:
            if attempt >= retries:
                raise RuntimeError(f"gemini timed out after {timeout_s * (2 ** attempt)}s")
            attempt += 1
            continue
        if result.returncode == 0:
            return result.stdout.strip()
        if attempt >= retries:
            raise RuntimeError(result.stderr.strip() or "gemini failed")
        attempt += 1


def normalize_json_output(text: str) -> str:
    stripped = text.strip()
    if "```" in stripped:
        stripped = stripped.replace("```json", "").replace("```", "").strip()
    if stripped.startswith("json"):
        stripped = stripped[4:].strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped
    if "\"thread_id\"" in stripped and "\"summary\"" in stripped:
        return "{\n" + stripped.strip().rstrip(",") + "\n}"
    return stripped


def repair_json_output(text: str) -> str:
    normalized = normalize_json_output(text)
    try:
        parsed = json.loads(normalized)
        return json.dumps(parsed, ensure_ascii=False)
    except Exception:
        pass

    def find_str(key: str) -> str | None:
        match = re.search(rf'"{key}"\\s*:\\s*"([^"]*)"', normalized, re.DOTALL)
        return match.group(1).strip() if match else None

    def find_status() -> str | None:
        match = re.search(
            r"\\b(open|resolved|duplicate|not_a_bug|feature_request|unclear)\\b",
            normalized,
            re.IGNORECASE,
        )
        return match.group(1).lower() if match else None

    def find_confidence() -> str | None:
        match = re.search(r"\\b(low|medium|high)\\b", normalized, re.IGNORECASE)
        return match.group(1).lower() if match else None

    def find_list(key: str) -> list[str]:
        block = None
        match = re.search(rf'"{key}"\\s*:\\s*\\[(.*?)\\]', normalized, re.DOTALL)
        if match:
            block = match.group(1)
        if not block:
            return []
        return [s.strip() for s in re.findall(r'"([^"]+)"', block)]

    repaired = {
        "thread_id": find_str("thread_id") or "",
        "summary": find_str("summary") or "",
        "status_guess": find_str("status_guess") or find_status() or "unclear",
        "confidence": find_str("confidence") or find_confidence() or "low",
        "evidence": find_list("evidence"),
        "duplicate_candidates": find_list("duplicate_candidates"),
    }
    return json.dumps(repaired, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/bbs.sqlite"))
    parser.add_argument("--thread-id", required=True)
    parser.add_argument("--max-posts", type=int, default=11)
    parser.add_argument("--json-only", action="store_true")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    t0 = time.monotonic()
    thread = load_thread(conn, args.thread_id, max_posts=args.max_posts)
    t_load = time.monotonic()
    if not args.json_only:
        print(f"thread_id: {thread['thread_id']}")
        print(f"title: {thread['title']}")
        for post in thread["posts"]:
            print(f"{post['post_id']} | {post['author']} | {post['posted_at']}")
            print(post["body"])
            print("-" * 60)
    prompt = build_prompt(thread)
    t_prompt = time.monotonic()
    output = run_gemini(prompt)
    t_llm = time.monotonic()
    if args.json_only:
        repaired = repair_json_output(output)
        payload = json.loads(repaired)
        payload["timings"] = {
            "load_s": round(t_load - t0, 6),
            "prompt_s": round(t_prompt - t_load, 6),
            "llm_s": round(t_llm - t_prompt, 6),
            "parse_s": round(time.monotonic() - t_llm, 6),
            "total_s": round(time.monotonic() - t0, 6),
        }
        print(json.dumps(payload, ensure_ascii=False))
    else:
        print("llm_output:")
        print(repair_json_output(output))


if __name__ == "__main__":
    main()

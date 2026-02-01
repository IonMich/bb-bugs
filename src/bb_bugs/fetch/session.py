
import time
from dataclasses import dataclass
from typing import Iterable

import requests

from bb_bugs.fetch.rate_limit import RateLimiter


@dataclass
class FetchConfig:
    min_delay_s: float = 2.5
    jitter_s: float = 2.5
    max_retries: int = 3
    timeout_s: float = 20.0
    user_agent: str = "bb-bugs-fetcher/0.1 (+polite; contact=local)"


class PoliteSession:
    def __init__(self, config: FetchConfig | None = None, *, limiter: RateLimiter | None = None) -> None:
        self.config = config or FetchConfig()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.config.user_agent})
        self.limiter = limiter or RateLimiter(self.config.min_delay_s, self.config.jitter_s)

    def get(self, url: str, *, allowed_statuses: Iterable[int] = (200,)) -> requests.Response:
        attempt = 0
        while True:
            self.limiter.wait()
            try:
                resp = self.session.get(url, timeout=self.config.timeout_s)
            except requests.RequestException:
                if attempt >= self.config.max_retries:
                    raise
                attempt += 1
                time.sleep(2**attempt)
                continue

            if resp.status_code in allowed_statuses:
                return resp

            if resp.status_code in (429, 502, 503, 504) and attempt < self.config.max_retries:
                attempt += 1
                time.sleep(2**attempt)
                continue

            resp.raise_for_status()

    def post(
        self, url: str, *, data: dict, allowed_statuses: Iterable[int] = (200,)
    ) -> requests.Response:
        attempt = 0
        while True:
            self.limiter.wait()
            try:
                resp = self.session.post(url, data=data, timeout=self.config.timeout_s)
            except requests.RequestException:
                if attempt >= self.config.max_retries:
                    raise
                attempt += 1
                time.sleep(2**attempt)
                continue

            if resp.status_code in allowed_statuses:
                return resp

            if resp.status_code in (429, 502, 503, 504) and attempt < self.config.max_retries:
                attempt += 1
                time.sleep(2**attempt)
                continue

            resp.raise_for_status()

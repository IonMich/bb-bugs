
import random
import time
from dataclasses import dataclass


@dataclass
class RateLimiter:
    min_delay_s: float
    jitter_s: float
    _last_request_ts: float | None = None

    def wait(self) -> None:
        if self._last_request_ts is None:
            self._last_request_ts = time.monotonic()
            return
        delay = self.min_delay_s + random.uniform(0, self.jitter_s)
        elapsed = time.monotonic() - self._last_request_ts
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_request_ts = time.monotonic()

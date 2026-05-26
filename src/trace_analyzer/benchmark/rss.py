import os
import threading
import time

import psutil


class RssSampler:
    def __init__(self, interval_s: float = 0.01):
        self.process = psutil.Process(os.getpid())
        self.interval_s = interval_s
        self.peak_bytes = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._sample, daemon=True)

    def __enter__(self):
        self.peak_bytes = self.process.memory_info().rss
        self._thread.start()
        return self

    def __exit__(self, type, value, traceback):
        self._stop.set()
        self._thread.join()
        self.peak_bytes = max(self.peak_bytes,
                              self.process.memory_info().rss)

    def _sample(self):
        while not self._stop.is_set():
            self.peak_bytes = max(
                self.peak_bytes,
                self.process.memory_info().rss,
            )
            time.sleep(self.interval_s)

    @property
    def peak_mb(self) -> float:
        return self.peak_bytes / 1024 / 1024

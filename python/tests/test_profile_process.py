"""Process-tree resource sampling (plan §2.7): RSS/CPU of the server +
workers, sampled during the measurement window."""

import os
import time

from lite_server.profile.process import sample_process_cpu_percent


class TestCpuSampler:
    def test_cpu_percent_reflects_actual_usage_between_calls(self):
        # AUDIT B2 (pure-function assumption): sample_process_cpu_percent
        # builds a NEW psutil.Process instance on every call. psutil's
        # cpu_percent(interval=None) is documented to return a meaningless
        # 0.0 on an instance's FIRST call — with a fresh instance every time,
        # every sample is 0.0, so cpu_mean is always 0.0 (silent fake data,
        # the exact failure mode plan §0.1 exists to eradicate).
        sample_process_cpu_percent(os.getpid())  # prime any caches
        samples = []
        for _ in range(2):
            end = time.perf_counter() + 0.3
            x = 0
            while time.perf_counter() < end:
                x += 1  # busy-burn CPU so the delta must be measurable
            samples.append(sample_process_cpu_percent(os.getpid()))
        assert any((s or 0.0) > 0.0 for s in samples), (
            f"cpu_percent must measure usage between calls, got {samples} — "
            "fresh psutil.Process instances make every call a 'first call' "
            "that returns 0.0"
        )

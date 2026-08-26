"""M4: report_accelerator_metrics helper + collect_metrics drain semantics."""

from lite_server.api import LitAPI
from lite_server.pipeline import collect_metrics


class _API(LitAPI):
    def predict(self, x):
        return x


def _api() -> _API:
    return _API()


class TestReportAcceleratorMetrics:
    def test_should_buffer_latest_reading_per_device(self):
        api = _api()
        api.report_accelerator_metrics("0", "cuda", utilization_percent=40.0)
        api.report_accelerator_metrics("0", "cuda", utilization_percent=55.0)
        api.report_accelerator_metrics("1", "cuda", utilization_percent=10.0)

        assert len(api._accelerator_readings) == 2
        assert api._accelerator_readings[("0", "cuda")]["utilization_percent"] == 55.0

    def test_should_omit_fields_not_reported(self):
        api = _api()
        api.report_accelerator_metrics("0", "npu", memory_used_bytes=1024)

        reading = api._accelerator_readings[("0", "npu")]
        assert reading == {"device": "0", "accel": "npu", "memory_used_bytes": 1024.0}

    def test_should_be_thread_safe_under_concurrent_reports(self):
        import threading

        api = _api()

        def report(device: str) -> None:
            for i in range(100):
                api.report_accelerator_metrics(device, "cuda", utilization_percent=float(i))

        threads = [threading.Thread(target=report, args=(str(d),)) for d in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(api._accelerator_readings) == 4


class TestCollectMetricsAccelerator:
    def test_should_attach_readings_to_metrics_proto(self):
        api = _api()
        api.report_accelerator_metrics(
            "0",
            "cuda",
            utilization_percent=42.5,
            memory_used_bytes=1.5e9,
            memory_total_bytes=8.0e9,
            temperature_celsius=65.0,
        )

        metrics = collect_metrics(api)

        assert metrics is not None
        assert len(metrics.accelerator) == 1
        reading = metrics.accelerator[0]
        assert reading.device == "0"
        assert reading.accel == "cuda"
        assert reading.utilization_percent == 42.5
        assert reading.memory_used_bytes == 1.5e9
        assert reading.memory_total_bytes == 8.0e9
        assert reading.temperature_celsius == 65.0

    def test_should_keep_unreported_fields_absent_in_proto(self):
        api = _api()
        api.report_accelerator_metrics("0", "mlu", utilization_percent=10.0)

        reading = collect_metrics(api).accelerator[0]

        assert reading.HasField("utilization_percent")
        assert not reading.HasField("temperature_celsius")
        assert not reading.HasField("memory_used_bytes")

    def test_should_drain_buffer_on_collect(self):
        api = _api()
        api.report_accelerator_metrics("0", "cuda", utilization_percent=1.0)

        assert collect_metrics(api) is not None
        assert api._accelerator_readings == {}
        # Nothing buffered anymore → no Metrics proto at all.
        assert collect_metrics(api) is None

    def test_should_return_none_when_no_metrics_and_no_readings(self):
        assert collect_metrics(_api()) is None

    def test_should_survive_lit_api_without_accelerator_buffer(self):
        class Legacy:
            _metric_lock = __import__("threading").Lock()
            _metric_values = []
            _metric_specs = []

        assert collect_metrics(Legacy()) is None

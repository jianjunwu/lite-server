"""Tests for lite_server.init.generator — ProjectGenerator."""

from pathlib import Path

import pytest

from lite_server.init.generator import ProjectGenerator


class TestProjectGeneratorBasics:
    """Construction and validation."""

    def test_model_name_defaults_to_my_model(self, tmp_path):
        gen = ProjectGenerator("proj", tmp_path)
        assert gen.model_name == "my_model"

    def test_custom_model_name(self, tmp_path):
        gen = ProjectGenerator("proj", tmp_path, model_name="foo")
        assert gen.model_name == "foo"


class TestProjectGeneration:
    """File generation."""

    def test_generates_all_expected_files(self, tmp_path):
        gen = ProjectGenerator("myproj", tmp_path, model_name="test_model")
        root = gen.generate()

        assert root.exists()
        assert (root / "server.yaml").exists()
        assert (root / "Dockerfile").exists()
        assert (root / "docker-compose.yml").exists()
        assert (root / "Makefile").exists()
        assert (root / "test_request.py").exists()
        assert (root / "README.md").exists()
        assert (root / "requirements.txt").exists()
        assert (root / ".gitignore").exists()
        assert (root / ".github" / "workflows" / "ci.yml").exists()
        assert (root / "model_repo" / "test_model" / "1" / "model.py").exists()
        assert (root / "model_repo" / "test_model" / "1" / "config.yaml").exists()
        assert (root / "model_repo" / "test_model" / "1" / "config.yaml.example").exists()

    def test_model_py_is_valid_python(self, tmp_path):
        gen = ProjectGenerator("myproj", tmp_path)
        root = gen.generate()
        model_py = root / "model_repo" / "my_model" / "1" / "model.py"
        compile(model_py.read_text(), str(model_py), "exec")

    def test_raises_if_directory_exists(self, tmp_path):
        existing = tmp_path / "exists"
        existing.mkdir()
        gen = ProjectGenerator("exists", tmp_path)
        with pytest.raises(FileExistsError):
            gen.generate()

    def test_returns_root_path(self, tmp_path):
        gen = ProjectGenerator("myproj", tmp_path)
        root = gen.generate()
        assert root == tmp_path / "myproj"


class TestServerYaml:
    """server.yaml content."""

    def test_contains_http_port(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "http_port: 8000" in text

    def test_contains_model_repo_path(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "path: ./model_repo" in text


class TestConfigYaml:
    """model version config.yaml content."""

    def test_empty_template_has_minimal_config(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        assert "max_queue_size: 1000" in text
        assert "request_timeout: 30.0" in text


class TestConfigYamlStructure:
    """config.yaml should be concise: active config on top, reference below."""

    def test_generates_config_yaml_example(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        example = root / "model_repo" / "my_model" / "1" / "config.yaml.example"
        assert example.exists(), "Should generate config.yaml.example as reference"

    def test_active_config_not_all_commented(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        lines = text.splitlines()
        active_lines = [l for l in lines if l.strip() and not l.strip().startswith("#")]
        assert len(active_lines) >= 1, "Active config should not be entirely commented out"

    def test_contains_request_timeout(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        assert "request_timeout:" in text

    def test_example_contains_all_fields(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        example = root / "model_repo" / "my_model" / "1" / "config.yaml.example"
        text = example.read_text()
        assert "max_batch_size" in text
        assert "stream" in text
        assert "accelerator" in text
        assert "hooks" in text

    def test_example_contains_on_error_http(self, tmp_path):
        """WorkerHooksConfig supports on_error_http; example should show it."""
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        example = root / "model_repo" / "my_model" / "1" / "config.yaml.example"
        text = example.read_text()
        assert "on_error_http" in text


class TestServerYamlEnhanced:
    """server.yaml should expose model_defaults, features and key defaults."""

    def test_contains_timeout_with_value(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "timeout: 30.0" in text

    def test_contains_graceful_timeout(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "graceful_timeout:" in text

    def test_contains_keepalive_timeout(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "keepalive_timeout:" in text

    def test_contains_model_defaults(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "model_defaults:" in text

    def test_contains_features(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "features:" in text

    def test_no_transport_field(self, tmp_path):
        """transport was removed from ServerConfig (commit 95518e0)."""
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "transport" not in text


class TestNoStandaloneOrchestration:
    """orchestration.yaml should NOT be generated standalone; server.yaml owns it."""

    def test_no_standalone_orchestration_yaml(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path, model_name="foo")
        root = gen.generate()
        assert not (root / "model_repo" / "orchestration.yaml").exists()


class TestDockerCompose:
    """docker-compose.yml should include healthcheck and restart."""

    def test_contains_restart_policy(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "docker-compose.yml").read_text()
        assert "restart:" in text

    def test_contains_healthcheck(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "docker-compose.yml").read_text()
        assert "healthcheck:" in text


class TestCIEnhanced:
    """CI workflow should validate model configs."""

    def test_validates_model_config(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path, model_name="foo")
        root = gen.generate()
        text = (root / ".github" / "workflows" / "ci.yml").read_text()
        assert "config.yaml" in text or "config-check" in text


class TestTestRequestPyEnhanced:
    """test_request.py should handle connection errors gracefully."""

    def test_has_error_handling(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "test_request.py").read_text()
        assert "try:" in text or "except" in text or "ConnectionError" in text


class TestMakefile:
    """Makefile targets."""

    def test_has_serve_target(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path, model_name="m")
        root = gen.generate()
        text = (root / "Makefile").read_text()
        assert "serve:" in text
        assert "lite-server serve" in text

    def test_has_test_target(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "Makefile").read_text()
        assert "test:" in text

    def test_has_benchmark_target_with_model_name(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path, model_name="foo")
        root = gen.generate()
        text = (root / "Makefile").read_text()
        assert "benchmark:" in text
        assert "--model foo" in text


class TestDockerfile:
    """Dockerfile content."""

    def test_exposes_ports(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "Dockerfile").read_text()
        assert "EXPOSE 8000" in text

    def test_has_lite_server_cmd(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "Dockerfile").read_text()
        assert 'lite-server", "serve"' in text or "lite-server serve" in text


class TestGitignore:
    """.gitignore content."""

    def test_ignores_pycache(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / ".gitignore").read_text()
        assert "__pycache__/" in text

    def test_ignores_model_weights(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / ".gitignore").read_text()
        assert "*.pt" in text


class TestServerYamlOrchestration:
    """server.yaml must contain orchestration config."""

    def test_contains_orchestration_section(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path, model_name="foo")
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "orchestration:" in text
        assert "load_models:" in text
        assert "- foo" in text


class TestModelPySafety:
    """Generated model.py must not crash on edge-case inputs."""

    def test_decode_request_without_input_key(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        model_py = root / "model_repo" / "my_model" / "1" / "model.py"
        code = compile(model_py.read_text(), str(model_py), "exec")
        namespace = {}
        exec(code, namespace)
        # Find the LitAPI subclass
        api_cls = None
        for obj in namespace.values():
            if isinstance(obj, type) and hasattr(obj, "predict") and obj.__name__ != "LitAPI":
                api_cls = obj
                break
        assert api_cls is not None
        instance = api_cls()
        # Since 0.7.0 decode_request is async and takes a ctx argument.
        import asyncio
        result = asyncio.run(instance.decode_request({"other": "value"}, None))
        assert result is not None  # Safe default, not a crash


class TestTestRequestPy:
    """Generated test_request.py must handle HTTP errors."""

    def test_has_status_code_check(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "test_request.py").read_text()
        assert "status_code" in text
        # Should branch on error rather than blindly calling resp.json()
        assert "if" in text or "try" in text or "raise_for_status" in text

    def test_fstring_interpolation_not_escaped(self, tmp_path):
        """f-string must interpolate BASE_URL variable, not print literal {BASE_URL}."""
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "test_request.py").read_text()
        # The template should NOT have {{BASE_URL}} (escaped braces) in f-strings
        # because the template itself is a regular string, not an f-string.
        assert "{{BASE_URL}}" not in text, "f-string braces are double-escaped"
        assert "{{e}}" not in text, "f-string braces are double-escaped"


class TestCallbacksPy:
    """callbacks.py example is always generated alongside model.py."""

    def test_callbacks_py_generated(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        cb = root / "model_repo" / "my_model" / "1" / "callbacks.py"
        assert cb.exists()

    def test_callbacks_py_defines_callback_subclass(self, tmp_path):
        gen = ProjectGenerator("p", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "my_model" / "1" / "callbacks.py").read_text()
        compile(text, "callbacks.py", "exec")
        assert "Callback" in text


class TestHotReloadTemplateContent:
    """Generated templates reflect the reconcile + FILE_CHANGED semantics:
    hot_reload restarts/refreshes already-loaded versions (on_file_changed
    hook for in-process refresh); version-dir lifecycle belongs to the
    reconciler; manual-mode auto-load is deprecated."""

    def _example(self, tmp_path):
        gen = ProjectGenerator("myproj", tmp_path)
        root = gen.generate()
        return (root / "model_repo" / "my_model" / "1" / "config.yaml.example").read_text()

    def test_config_example_mentions_on_file_changed_hook(self, tmp_path):
        assert "on_file_changed" in self._example(tmp_path)

    def test_config_example_notes_deprecated_manual_autoload(self, tmp_path):
        assert "deprecated" in self._example(tmp_path).lower()

    def test_model_py_shows_on_file_changed_example(self, tmp_path):
        gen = ProjectGenerator("myproj", tmp_path)
        root = gen.generate()
        model_py = (root / "model_repo" / "my_model" / "1" / "model.py").read_text()
        assert "on_file_changed" in model_py

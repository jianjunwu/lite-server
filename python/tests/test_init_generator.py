"""Tests for lite_server.init.generator — ProjectGenerator."""

from pathlib import Path

import pytest

from lite_server.init.generator import ProjectGenerator, TEMPLATES


class TestProjectGeneratorBasics:
    """Construction and validation."""

    def test_unknown_template_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown template"):
            ProjectGenerator("proj", "not_a_template", tmp_path)

    def test_all_templates_are_known(self):
        assert set(TEMPLATES) == {"empty", "llm", "cv-classify", "cv-detect", "nlp"}

    def test_model_name_defaults_to_my_model(self, tmp_path):
        gen = ProjectGenerator("proj", "empty", tmp_path)
        assert gen.model_name == "my_model"

    def test_custom_model_name(self, tmp_path):
        gen = ProjectGenerator("proj", "empty", tmp_path, model_name="foo")
        assert gen.model_name == "foo"


class TestProjectGeneration:
    """File generation across all templates."""

    @pytest.fixture(params=list(TEMPLATES))
    def template(self, request):
        return request.param

    def test_generates_all_expected_files(self, tmp_path, template):
        gen = ProjectGenerator("myproj", template, tmp_path, model_name="test_model")
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
        assert (root / "model_repo" / "orchestration.yaml").exists()

    def test_model_py_is_valid_python(self, tmp_path, template):
        gen = ProjectGenerator("myproj", template, tmp_path)
        root = gen.generate()
        model_py = root / "model_repo" / "my_model" / "1" / "model.py"
        compile(model_py.read_text(), str(model_py), "exec")

    def test_raises_if_directory_exists(self, tmp_path):
        existing = tmp_path / "exists"
        existing.mkdir()
        gen = ProjectGenerator("exists", "empty", tmp_path)
        with pytest.raises(FileExistsError):
            gen.generate()

    def test_returns_root_path(self, tmp_path, template):
        gen = ProjectGenerator("myproj", template, tmp_path)
        root = gen.generate()
        assert root == tmp_path / "myproj"


class TestServerYaml:
    """server.yaml content."""

    def test_contains_http_port(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "http_port: 8000" in text

    def test_contains_model_repo_path(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / "server.yaml").read_text()
        assert "path: ./model_repo" in text


class TestConfigYaml:
    """model version config.yaml content."""

    def test_contains_api_path(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path, model_name="m")
        root = gen.generate()
        text = (root / "model_repo" / "m" / "1" / "config.yaml").read_text()
        assert "api_path: /predict" in text

    def test_llm_template_has_stream_true(self, tmp_path):
        gen = ProjectGenerator("p", "llm", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        assert "stream: true" in text

    def test_cv_detect_template_has_batch_size(self, tmp_path):
        gen = ProjectGenerator("p", "cv-detect", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "my_model" / "1" / "config.yaml").read_text()
        assert "max_batch_size" in text


class TestMakefile:
    """Makefile targets."""

    def test_has_serve_target(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path, model_name="m")
        root = gen.generate()
        text = (root / "Makefile").read_text()
        assert "serve:" in text
        assert "lite-server serve" in text

    def test_has_test_target(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / "Makefile").read_text()
        assert "test:" in text

    def test_has_benchmark_target_with_model_name(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path, model_name="foo")
        root = gen.generate()
        text = (root / "Makefile").read_text()
        assert "benchmark:" in text
        assert "--model foo" in text


class TestOrchestrationYaml:
    """orchestration.yaml content."""

    def test_lists_model_in_load_models(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path, model_name="foo")
        root = gen.generate()
        text = (root / "model_repo" / "orchestration.yaml").read_text()
        assert "- foo" in text

    def test_control_mode_explicit(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / "model_repo" / "orchestration.yaml").read_text()
        assert "control_mode: explicit" in text


class TestDockerfile:
    """Dockerfile content."""

    def test_exposes_ports(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / "Dockerfile").read_text()
        assert "EXPOSE 8000" in text

    def test_has_lite_server_cmd(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / "Dockerfile").read_text()
        assert 'lite-server", "serve"' in text or "lite-server serve" in text


class TestGitignore:
    """.gitignore content."""

    def test_ignores_pycache(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / ".gitignore").read_text()
        assert "__pycache__/" in text

    def test_ignores_model_weights(self, tmp_path):
        gen = ProjectGenerator("p", "empty", tmp_path)
        root = gen.generate()
        text = (root / ".gitignore").read_text()
        assert "*.pt" in text

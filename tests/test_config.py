from pathlib import Path

import pytest
from typer.testing import CliRunner

from catalog_pii_scanner.cli import app
from catalog_pii_scanner.config import load_config_from_file


@pytest.fixture()
def valid_config_yaml() -> str:
    return (
        "ai:\n"
        "  mode: ensemble\n"
        "  ner:\n"
        "    enabled: true\n"
        "    provider: presidio\n"
        "    confidence_min: 0.6\n"
        "  embeddings:\n"
        "    enabled: true\n"
        "    model: sentence-transformers/all-MiniLM-L6-v2\n"
        "    device: cpu\n"
        "    features:\n"
        "      from_metadata: true\n"
        "      from_samples: true\n"
        "  ensemble:\n"
        "    weights:\n"
        "      rules: 0.4\n"
        "      ner: 0.3\n"
        "      embed: 0.3\n"
        "    decision_threshold: 0.55\n"
        "  llm:\n"
        "    enabled: false\n"
        "    provider: local\n"
        "    model: llama3-8b-instruct\n"
        "    max_tokens: 256\n"
        "    temperature: 0.0\n"
        "    redact: true\n"
        "    cost_cap_usd_per_scan: 0.5\n"
        "    cache_ttl_minutes: 1440\n"
    )


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_config_validate_valid(tmp_path: Path, valid_config_yaml: str) -> None:
    cfg_path = _write(tmp_path / "config.yaml", valid_config_yaml)
    runner = CliRunner()
    res = runner.invoke(app, ["config", "validate", "-f", str(cfg_path)])
    assert res.exit_code == 0
    assert "Config OK" in res.stdout


@pytest.mark.parametrize(
    "bad_yaml,expect_snippet",
    [
        ("{}\n", "ai"),
        ("ai:\n  mode: nope\n", "mode"),
        (
            "ai:\n  ensemble:\n    decision_threshold: 1.5\n",
            "decision_threshold",
        ),
        (
            "ai:\n  llm:\n    provider: unknown\n",
            "provider",
        ),
        (
            "ai:\n  weights: {invalid: 1.0}\n",
            "Extra inputs",
        ),
        (
            "ai:\n  ner:\n    enabled: [1,2]\n",
            "ner.enabled",
        ),
    ],
)
def test_config_validate_invalid(tmp_path: Path, bad_yaml: str, expect_snippet: str) -> None:
    cfg_path = _write(tmp_path / "bad.yaml", bad_yaml)
    runner = CliRunner()
    res = runner.invoke(app, ["config", "validate", "-f", str(cfg_path)])
    assert res.exit_code != 0
    assert "Config invalid" in res.stdout
    assert expect_snippet in res.stdout


def test_env_overrides_loader(
    tmp_path: Path, valid_config_yaml: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_path = _write(tmp_path / "config.yaml", valid_config_yaml)
    # Override a few values via env
    monkeypatch.setenv("CPS_AI__MODE", "ensemble+llm")
    monkeypatch.setenv("CPS_AI__NER__ENABLED", "false")
    monkeypatch.setenv("CPS_AI__LLM__PROVIDER", "openai")

    cfg = load_config_from_file(cfg_path)
    assert cfg.ai.mode == "ensemble+llm"
    assert cfg.ai.ner.enabled is False
    assert cfg.ai.llm.provider == "openai"

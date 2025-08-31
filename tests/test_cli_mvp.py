import json
import os
from pathlib import Path

from typer.testing import CliRunner

from catalog_pii_scanner.cli import app


def test_cli_scan_text_json_output() -> None:
    os.environ["CPS_OFFLINE"] = "1"
    runner = CliRunner()
    text = "Reach me at john.doe@example.com"
    result = runner.invoke(app, ["scan-text", text])
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert all("probs" in p for p in data)


def test_cli_train_calibrate_eval(tmp_path: Path) -> None:
    os.environ["CPS_OFFLINE"] = "1"
    runner = CliRunner()

    synth = tmp_path / "synth.jsonl"
    models = tmp_path / ".models"

    r1 = runner.invoke(app, ["gen-synth", str(synth), "--n", "20", "--seed", "1"])
    assert r1.exit_code == 0 and synth.exists()

    r2 = runner.invoke(app, ["train-embed", str(synth), "--model-dir", str(models)])
    assert r2.exit_code == 0
    # embed model may or may not save depending on deps, but command should succeed

    r3 = runner.invoke(app, ["calibrate", str(synth), "--model-dir", str(models)])
    assert r3.exit_code == 0

    r4 = runner.invoke(app, ["eval", str(synth), "--model-dir", str(models)])
    assert r4.exit_code == 0
    assert "Micro:" in r4.stdout and "Macro:" in r4.stdout

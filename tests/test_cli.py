from typer.testing import CliRunner

from catalog_pii_scanner.cli import app


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Catalog PII Scanner CLI" in result.stdout
    assert "scan" in result.stdout
    assert "serve" in result.stdout

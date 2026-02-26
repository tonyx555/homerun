import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import config


def test_database_url_default_is_postgres_asyncpg():
    default_database_url = config.Settings.model_fields["DATABASE_URL"].default
    assert str(default_database_url).startswith("postgresql+asyncpg://")


def test_normalize_database_url_trims_quotes_whitespace_and_trailing_slash():
    normalized = config.Settings._normalize_database_url(
        "  'postgresql+asyncpg://homerun:homerun@127.0.0.1:5432/homerun/'  "
    )
    assert normalized == "postgresql+asyncpg://homerun:homerun@127.0.0.1:5432/homerun"


def test_normalize_database_url_preserves_none_and_empty():
    assert config.Settings._normalize_database_url(None) is None
    assert config.Settings._normalize_database_url("   ") == ""


def test_normalize_database_url_strips_utf8_bom():
    normalized = config.Settings._normalize_database_url(
        "\ufeffpostgresql+asyncpg://homerun:homerun@127.0.0.1:5432/homerun"
    )
    assert normalized == "postgresql+asyncpg://homerun:homerun@127.0.0.1:5432/homerun"


def test_read_runtime_database_url_normalizes_value(tmp_path, monkeypatch):
    runtime_path = tmp_path / "database_url"
    runtime_path.write_text(
        "  'postgresql+asyncpg://homerun:homerun@127.0.0.1:5433/homerun/'  ",
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "_RUNTIME_DATABASE_URL_PATH", runtime_path)
    assert (
        config._read_runtime_database_url()
        == "postgresql+asyncpg://homerun:homerun@127.0.0.1:5433/homerun"
    )


def test_read_runtime_database_url_returns_none_when_missing(tmp_path, monkeypatch):
    runtime_path = tmp_path / "missing_database_url"
    monkeypatch.setattr(config, "_RUNTIME_DATABASE_URL_PATH", runtime_path)
    assert config._read_runtime_database_url() is None


def test_default_database_url_prefers_runtime_value(tmp_path, monkeypatch):
    runtime_path = tmp_path / "database_url"
    runtime_path.write_text(
        "postgresql+asyncpg://homerun:homerun@127.0.0.1:5434/homerun",
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "_RUNTIME_DATABASE_URL_PATH", runtime_path)
    assert (
        config._default_database_url()
        == "postgresql+asyncpg://homerun:homerun@127.0.0.1:5434/homerun"
    )

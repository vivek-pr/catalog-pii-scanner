from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, cast

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, Field, ValidationError, field_validator


class NERConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    enabled: bool = True
    provider: Literal["presidio", "spacy"] = "presidio"
    confidence_min: float = Field(0.60, ge=0.0, le=1.0)
    # Language code hint (e.g., 'en'). For spaCy, optionally specify model name.
    language: str = "en"
    spacy_model: str | None = None


class EmbeddingsFeaturesConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    from_metadata: bool = True
    from_samples: bool = True


class EmbeddingsConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    enabled: bool = True
    model: str = "sentence-transformers/all-MiniLM-L6-v2"
    device: Literal["cpu", "cuda"] = "cpu"
    features: EmbeddingsFeaturesConfig = Field(default_factory=lambda: EmbeddingsFeaturesConfig())


class EnsembleWeights(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    rules: float = Field(0.4, ge=0.0, le=1.0)
    ner: float = Field(0.3, ge=0.0, le=1.0)
    embed: float = Field(0.3, ge=0.0, le=1.0)

    @field_validator("rules", "ner", "embed")
    @classmethod
    def _non_nan(cls, v: float) -> float:
        if v is None or not isinstance(v, float | int):
            raise ValueError("weight must be a number")
        return float(v)


class EnsembleConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    weights: EnsembleWeights = Field(
        default_factory=lambda: EnsembleWeights(rules=0.4, ner=0.3, embed=0.3)
    )
    decision_threshold: float = Field(0.55, ge=0.0, le=1.0)


class LLMConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    enabled: bool = False
    provider: Literal["local", "openai", "azure", "vertex"] = "local"
    model: str = "llama3-8b-instruct"
    max_tokens: int = Field(256, ge=1)
    temperature: float = Field(0.0, ge=0.0, le=2.0)
    redact: bool = True
    cost_cap_usd_per_scan: float = Field(0.50, ge=0.0)
    cache_ttl_minutes: int = Field(1440, ge=0)


class AIConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    mode: Literal["rules", "ensemble", "ensemble+llm"] = "ensemble"
    ner: NERConfig = Field(
        default_factory=lambda: NERConfig(enabled=True, provider="presidio", confidence_min=0.60)
    )
    embeddings: EmbeddingsConfig = Field(
        default_factory=lambda: EmbeddingsConfig(
            enabled=True,
            model="sentence-transformers/all-MiniLM-L6-v2",
            device="cpu",
            features=EmbeddingsFeaturesConfig(from_metadata=True, from_samples=True),
        )
    )
    ensemble: EnsembleConfig = Field(
        default_factory=lambda: EnsembleConfig(
            weights=EnsembleWeights(rules=0.4, ner=0.3, embed=0.3), decision_threshold=0.55
        )
    )
    llm: LLMConfig = Field(
        default_factory=lambda: LLMConfig(
            enabled=False,
            provider="local",
            model="llama3-8b-instruct",
            max_tokens=256,
            temperature=0.0,
            redact=True,
            cost_cap_usd_per_scan=0.50,
            cache_ttl_minutes=1440,
        )
    )


class AppConfig(BaseModel):
    model_config = {
        "extra": "forbid",
    }
    ai: AIConfig


def _deep_update(dst: dict[str, Any], src: dict[str, Any]) -> dict[str, Any]:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            dst[k] = _deep_update(dst.get(k, {}), v)
        else:
            dst[k] = v
    return dst


def _parse_env_overrides(prefix: str = "CPS_") -> dict[str, Any]:
    """Parse environment variables into a nested dict using double underscore as a separator.

    Example:
      CPS_AI__NER__ENABLED=false -> {"ai": {"ner": {"enabled": False}}}
    """
    out: dict[str, Any] = {}
    plen = len(prefix)
    for key, raw in os.environ.items():
        if not key.startswith(prefix):
            continue
        path = key[plen:]
        if "__" not in path:
            # Only consider nested keys for config overrides
            continue
        parts = [p.strip().lower() for p in path.split("__") if p.strip()]
        if not parts:
            continue
        # best-effort parse of primitive types
        val: Any
        low = raw.strip().lower()
        if low in {"true", "1", "yes", "on"}:
            val = True
        elif low in {"false", "0", "no", "off"}:
            val = False
        else:
            try:
                if "." in raw:
                    val = float(raw)
                else:
                    val = int(raw)
            except Exception:
                val = raw
        node = out
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node[parts[-1]] = val
    return out


def load_config_from_file(path: str | Path | None, env_prefix: str = "CPS_") -> AppConfig:
    """Load config from YAML file and apply environment overrides.

    Raises ValidationError on invalid configuration.
    """
    data: dict[str, Any] = {}
    if path:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        with p.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
            if not isinstance(loaded, dict):
                raise ValueError("Top-level YAML must be a mapping/object")
            data = loaded
    overrides = _parse_env_overrides(prefix=env_prefix)
    if overrides:
        data = _deep_update(data, overrides)
    # pydantic typing for model_validate may be Any in some environments; cast for mypy
    return cast(AppConfig, AppConfig.model_validate(data))


def validate_config_file(
    path: str | Path, env_prefix: str = "CPS_"
) -> tuple[AppConfig | None, list[str]]:
    """Validate a config file and return (config, errors)."""
    try:
        cfg = load_config_from_file(path, env_prefix=env_prefix)
        return cfg, []
    except FileNotFoundError as e:
        return None, [str(e)]
    except ValidationError as e:  # pydantic errors
        msgs: list[str] = []
        for err in e.errors(include_url=False):  # type: ignore[no-typed-call]
            loc = ".".join(str(p) for p in err.get("loc", []))
            msg = err.get("msg", "Invalid value")
            msgs.append(f"{loc}: {msg}")
        return None, msgs
    except Exception as e:
        return None, [f"Error: {e}"]

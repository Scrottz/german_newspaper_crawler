from __future__ import annotations
import os
from typing import Any, Dict, Optional

import yaml


def get_config_path() -> str:
    """Return absolute path to `configs/config.yaml` relative to project root."""
    package_dir = os.path.dirname(__file__)  # e.g. .../lib/common
    project_root = os.path.abspath(os.path.join(package_dir, os.pardir, os.pardir))
    return os.path.join(project_root, "configs", "config.yaml")


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load YAML config from the given path (or from the default config path).
    Returns a dict; on error returns an empty dict.
    """
    cfg_path = path or get_config_path()
    try:
        with open(cfg_path, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def main() -> int:
    """Small CLI: load config and print top-level keys when executed directly."""
    cfg = load_config()
    print("Loaded config keys:", list(cfg.keys()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

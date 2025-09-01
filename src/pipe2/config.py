import json
from pathlib import Path


def load_config(path: str) -> dict:
    cfg_path = Path(path)
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def resolve_path(base: str, *parts: str) -> str:
    return str(Path(base, *parts))


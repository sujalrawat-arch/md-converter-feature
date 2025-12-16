from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()[:10]


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def write_json(path: str, obj: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def read_json(path: str, default: Any = None) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

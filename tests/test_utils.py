import json
import os

from pdf_extractor import utils


def test_ensure_dir_creates_path(tmp_path):
    target = tmp_path / "nested" / "dir"
    result = utils.ensure_dir(str(target))
    assert os.path.isdir(result)
    assert os.path.isdir(target)


def test_write_and_read_json_roundtrip(tmp_path):
    payload = {"foo": "bar", "nested": {"answer": 42}}
    target = tmp_path / "data.json"
    utils.write_json(str(target), payload)
    with open(target, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    assert raw == payload
    loaded = utils.read_json(str(target))
    assert loaded == payload


def test_read_json_missing_returns_default(tmp_path):
    missing = tmp_path / "missing.json"
    sentinel = {"default": True}
    assert utils.read_json(str(missing), sentinel) == sentinel

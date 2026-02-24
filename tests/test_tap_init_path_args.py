"""Tests for TapMassive init argument normalization."""

import json
import warnings

from tap_massive.tap import TapMassive


def test_catalog_and_state_path_args_do_not_emit_sdk_deprecation(tmp_path):
    """Tap should normalize path args to dicts before delegating to Singer SDK."""
    catalog_path = tmp_path / "catalog.json"
    state_path = tmp_path / "state.json"
    catalog_path.write_text(json.dumps({"streams": []}), encoding="utf-8")
    state_path.write_text(json.dumps({}), encoding="utf-8")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        TapMassive(
            config={"api_key": "test_key"},
            catalog=str(catalog_path),
            state=str(state_path),
        )

    warning_messages = [str(warning.message) for warning in caught]
    assert not any(
        "Passing a catalog file path is deprecated" in msg
        for msg in warning_messages
    )
    assert not any(
        "Passing a state file path is deprecated" in msg for msg in warning_messages
    )

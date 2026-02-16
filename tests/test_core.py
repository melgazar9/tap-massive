"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_massive.tap import TapMassive

SAMPLE_CONFIG = {
    "api_key": "test_key",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapMassive = get_tap_test_class(
    tap_class=TapMassive,
    config=SAMPLE_CONFIG,
)

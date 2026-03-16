"""Tests for stock stream post-processing and record normalization."""

from __future__ import annotations

from datetime import datetime, timezone

from tap_massive.stock_streams import StockIPOsStream, StockTickerEventsStream


class TestStockIPOsStream:
    def test_post_process_parses_last_updated_and_snake_cases(self):
        stream = StockIPOsStream.__new__(StockIPOsStream)
        row = {
            "ipoStatus": "NEW",
            "lastUpdated": "2026-02-19",
            "ticker": "TEST",
        }

        result = stream.post_process(row)

        assert result["ipo_status"] == "new"
        assert result["last_updated"] == datetime(
            2026, 2, 19, 0, 0, tzinfo=timezone.utc
        )

    def test_post_process_drops_unparseable_last_updated(self):
        stream = StockIPOsStream.__new__(StockIPOsStream)
        row = {
            "ipo_status": "pending",
            "last_updated": "not-a-date",
            "ticker": "TEST",
        }

        result = stream.post_process(row)

        assert "last_updated" not in result


class TestStockTickerEventsStream:
    def test_primary_keys(self):
        assert StockTickerEventsStream.primary_keys == ["ticker", "date", "type"]

    def test_parse_response_preserves_api_fields(self):
        stream = StockTickerEventsStream.__new__(StockTickerEventsStream)
        api_response = {
            "name": "Meta Platforms, Inc. Class A Common Stock",
            "events": [
                {
                    "date": "2022-06-09",
                    "type": "ticker_change",
                    "ticker_change": {"ticker": "META"},
                },
                {
                    "date": "2012-05-18",
                    "type": "ticker_change",
                    "ticker_change": {"ticker": "FB"},
                },
            ],
        }

        records = list(stream.parse_response(api_response, context={}))

        assert records == [
            {
                "name": "Meta Platforms, Inc. Class A Common Stock",
                "date": "2022-06-09",
                "type": "ticker_change",
                "ticker_change": {"ticker": "META"},
            },
            {
                "name": "Meta Platforms, Inc. Class A Common Stock",
                "date": "2012-05-18",
                "type": "ticker_change",
                "ticker_change": {"ticker": "FB"},
            },
        ]

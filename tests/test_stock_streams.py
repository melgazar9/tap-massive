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
    def test_primary_keys_do_not_require_event_type(self):
        assert StockTickerEventsStream.primary_keys == ["date", "name"]

    def test_parse_response_maps_type_alias_to_event_type(self):
        stream = StockTickerEventsStream.__new__(StockTickerEventsStream)
        raw = {
            "name": "ticker_change",
            "events": [
                {
                    "date": "2026-02-19",
                    "type": "ticker_change",
                    "ticker_change": {"ticker": "ABC"},
                }
            ],
        }

        records = list(stream.parse_response(raw, context={}))

        assert records == [
            {
                "name": "ticker_change",
                "date": "2026-02-19",
                "event_type": "ticker_change",
                "ticker_change": {"ticker": "ABC"},
            }
        ]

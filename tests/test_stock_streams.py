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
        assert StockTickerEventsStream.primary_keys == ["ticker"]

    def test_schema_preserves_events_array(self):
        schema_props = StockTickerEventsStream.schema["properties"]
        assert set(schema_props.keys()) == {
            "ticker",
            "name",
            "composite_figi",
            "cik",
            "events",
        }
        assert schema_props["events"]["type"] == ["array", "null"]
        event_item_props = schema_props["events"]["items"]["properties"]
        assert set(event_item_props.keys()) >= {"date", "type", "ticker_change"}

    def test_post_process_sets_ticker_from_context(self):
        stream = StockTickerEventsStream.__new__(StockTickerEventsStream)
        row = {
            "name": "Meta Platforms, Inc. Class A Common Stock",
            "composite_figi": "BBG000MM2P62",
            "cik": "0001326801",
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
        context = {"ticker": "META"}

        result = stream.post_process(row, context)

        assert result["ticker"] == "META"
        assert result["name"] == "Meta Platforms, Inc. Class A Common Stock"
        assert result["composite_figi"] == "BBG000MM2P62"
        assert result["cik"] == "0001326801"
        assert result["events"] == [
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
        ]

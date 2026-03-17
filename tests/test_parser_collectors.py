from app.models.enums import SourceChannel
from app.models.parser_source import ParserSource
from app.services import parser_collectors


def _source(extra_config: dict | None = None) -> ParserSource:
    return ParserSource(
        agency_id=1,
        name="Test TG",
        source_channel=SourceChannel.telegram,
        source_url="https://t.me/s/mychannel",
        city="Izhevsk",
        region_code="RU-UDM",
        is_active=True,
        poll_minutes=1440,
        max_items_per_run=10,
        extra_config=extra_config,
    )


def test_collect_telegram_items_from_widget_html_with_filters(monkeypatch):
    text = (
        "\u0421\u0434\u0430\u043c \u043e\u0444\u0438\u0441 120 \u043c2 \u0432 \u0418\u0436\u0435\u0432\u0441\u043a\u0435. "
        "\u0422\u0435\u043b\u0435\u0444\u043e\u043d +7 900 111-22-33. \u0426\u0435\u043d\u0430 150000 \u0440\u0443\u0431"
    )
    html = f"""
    <html><body>
      <div class=\"tgme_widget_message\" data-post=\"mychannel/101\">
        <a class=\"tgme_widget_message_date\" href=\"https://t.me/mychannel/101\">date</a>
        <div class=\"tgme_widget_message_text\">{text}</div>
      </div>
    </body></html>
    """

    monkeypatch.setattr(parser_collectors, "_fetch_text", lambda _: html)
    items = parser_collectors.collect_items_for_source(_source())

    assert len(items) == 1
    assert items[0].source_external_id == "mychannel/101"
    assert items[0].telegram_post_url == "https://t.me/mychannel/101"
    assert items[0].contact_phone is not None
    assert items[0].price_rub == 150000.0
    assert items[0].area_sqm == 120.0


def test_collect_telegram_items_filters_out_non_udmurtia_messages(monkeypatch):
    text = "\u041f\u0440\u043e\u0434\u0430\u043c \u043e\u0444\u0438\u0441 150 \u043c2 \u0432 \u041c\u043e\u0441\u043a\u0432\u0435"
    html = f"""
    <html><body>
      <div class=\"tgme_widget_message\" data-post=\"mychannel/102\">
        <a class=\"tgme_widget_message_date\" href=\"https://t.me/mychannel/102\">date</a>
        <div class=\"tgme_widget_message_text\">{text}</div>
      </div>
    </body></html>
    """

    monkeypatch.setattr(parser_collectors, "_fetch_text", lambda _: html)
    items = parser_collectors.collect_items_for_source(_source())
    assert items == []


def test_telegram_search_queries_uses_defaults_for_empty_input():
    queries = parser_collectors._telegram_search_queries([])
    assert len(queries) >= 3


def test_telegram_search_queries_deduplicates_and_limits():
    queries = parser_collectors._telegram_search_queries(["#A", "#a", "office izhevsk", "office izhevsk"])
    assert queries == ("#A", "office izhevsk")


def test_collect_telegram_api_search_requires_credentials():
    source = _source(
        {
            "mode": "telegram_api_search",
            "telegram_search": {"queries": ["#office"]},
            "telegram_filters": {"commercial_only": False, "udmurtia_only": False},
        }
    )
    parser_collectors.settings.telegram_api_id = ""
    parser_collectors.settings.telegram_api_hash = ""
    parser_collectors.settings.telegram_session_string = ""

    try:
        parser_collectors.collect_items_for_source(source)
        assert False, "Expected ValueError for missing TELEGRAM_API_ID/API_HASH"
    except ValueError as exc:
        assert "TELEGRAM_API_ID" in str(exc)

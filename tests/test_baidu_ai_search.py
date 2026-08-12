# -*- coding: utf-8 -*-
"""Contract tests for the Baidu Qianfan AI Search integration."""

import os
from datetime import date
from unittest.mock import patch

from src.config import Config
from src.search_service import (
    BaiduAISearchProvider,
    BochaSearchProvider,
    SearchResponse,
    SearchResult,
    SearchService,
)


class _FakeResponse:
    def __init__(self, payload, *, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    @property
    def headers(self):
        return {"content-type": "application/json"}


def test_bocha_provider_uses_current_official_endpoint() -> None:
    provider = BochaSearchProvider(["test-secret"])
    with patch(
        "src.search_service._post_with_retry",
        return_value=_FakeResponse(
            {"code": 200, "data": {"webPages": {"value": []}}}
        ),
    ) as request:
        response = provider.search("贵州茅台 最新公告", max_results=5, days=3)

    assert response.success is True
    assert request.call_args.args[0] == "https://api.bochaai.com/v1/web-search"


def test_config_loads_baidu_ai_search_keys() -> None:
    with (
        patch("src.config.setup_env"),
        patch.object(Config, "_parse_litellm_yaml", return_value=[]),
        patch.dict(
            os.environ,
            {
                "STOCK_LIST": "600519",
                "BAIDU_AI_SEARCH_API_KEYS": "key-one, key-two",
            },
            clear=True,
        ),
    ):
        config = Config._load_from_env()

    assert config.baidu_ai_search_api_keys == ["key-one", "key-two"]
    assert config.has_search_capability_enabled() is True


def test_baidu_provider_uses_v2_web_search_and_maps_references() -> None:
    provider = BaiduAISearchProvider(["test-secret"])
    payload = {
        "choices": [{"message": {"content": "summary", "role": "assistant"}}],
        "is_safe": True,
        "references": [
            {
                "id": "1",
                "title": "贵州茅台公告",
                "url": "https://example.cn/disclosure/1",
                "website": "示例交易所",
                "content": "公司发布最新公告。",
                "date": "2026-08-12",
                "type": "web",
            },
            {
                "id": "2",
                "title": "duplicate",
                "url": "https://example.cn/disclosure/1",
                "content": "duplicate",
                "type": "web",
            },
            {
                "id": "3",
                "title": "image",
                "url": "https://example.cn/image/1",
                "type": "image",
            },
        ],
    }

    with patch(
        "src.search_service._post_with_retry",
        return_value=_FakeResponse(payload),
    ) as request:
        response = provider.search("贵州茅台 600519 最新公告", max_results=5, days=3)

    assert response.success is True
    assert response.provider == "BaiduAI"
    assert len(response.results) == 1
    assert response.results[0].title == "贵州茅台公告"
    assert response.results[0].snippet == "公司发布最新公告。"
    assert response.results[0].source == "示例交易所"
    assert response.results[0].published_date == "2026-08-12"

    _, kwargs = request.call_args
    assert kwargs["headers"] == {
        "Authorization": "Bearer test-secret",
        "Content-Type": "application/json",
    }
    assert kwargs["json"] == {
        "messages": [
            {"role": "user", "content": "贵州茅台 600519 最新公告"}
        ],
        "search_source": "baidu_search_v2",
        "resource_type_filter": [{"type": "web", "top_k": 5}],
        "search_recency_filter": "week",
        "stream": False,
    }


def test_baidu_provider_maps_recency_and_caps_top_k() -> None:
    provider = BaiduAISearchProvider(["test-secret"])
    with patch(
        "src.search_service._post_with_retry",
        return_value=_FakeResponse({"references": []}),
    ) as request:
        response = provider.search("query", max_results=99, days=31)

    assert response.success is True
    _, kwargs = request.call_args
    assert kwargs["json"]["resource_type_filter"] == [
        {"type": "web", "top_k": 20}
    ]
    assert kwargs["json"]["search_recency_filter"] == "semiyear"


def test_baidu_provider_fails_closed_on_error_without_echoing_secret() -> None:
    provider = BaiduAISearchProvider(["never-log-this-secret"])
    with patch(
        "src.search_service._post_with_retry",
        return_value=_FakeResponse(
            {"code": 216003, "message": "authentication failed"},
            status_code=401,
        ),
    ):
        response = provider.search("query", max_results=3, days=3)

    assert response.success is False
    assert response.results == []
    assert response.error_message == (
        "HTTP 401, code=216003: authentication failed"
    )
    assert "never-log-this-secret" not in response.error_message


def test_search_service_places_baidu_after_bocha_before_existing_fallbacks() -> None:
    service = SearchService(
        bocha_keys=["bocha"],
        baidu_ai_search_keys=["baidu"],
        tavily_keys=["tavily"],
        brave_keys=["brave"],
        searxng_public_instances_enabled=False,
    )

    assert [provider.name for provider in service._providers] == [
        "Bocha",
        "BaiduAI",
        "Tavily",
        "Brave",
    ]


def test_a_share_search_fuses_bocha_and_baidu_and_prefers_financial_media() -> None:
    service = SearchService(
        bocha_keys=["bocha"],
        baidu_ai_search_keys=["baidu"],
        searxng_public_instances_enabled=False,
    )
    published = date.today().isoformat()
    community_url = "https://guba.eastmoney.com/news,600519,1.html"
    bocha_response = SearchResponse(
        query="贵州茅台 600519 股票 最新消息",
        provider="Bocha",
        success=True,
        results=[
            SearchResult(
                title="贵州茅台 600519 最新公告讨论",
                snippet="社区讨论贵州茅台公告",
                url=community_url,
                source="东方财富股吧",
                published_date=published,
            )
        ],
    )
    baidu_response = SearchResponse(
        query=bocha_response.query,
        provider="BaiduAI",
        success=True,
        results=[
            SearchResult(
                title="贵州茅台 600519 发布最新公告",
                snippet="公司披露最新经营信息",
                url="https://www.cnstock.com/commonDetail/123",
                source="上海证券报",
                published_date=published,
            ),
            SearchResult(
                title="duplicate",
                snippet="duplicate",
                url=community_url,
                source="东方财富股吧",
                published_date=published,
            ),
        ],
    )

    with (
        patch.object(service._providers[0], "search", return_value=bocha_response) as bocha,
        patch.object(service._providers[1], "search", return_value=baidu_response) as baidu,
    ):
        response = service.search_stock_news("600519", "贵州茅台", max_results=5)

    assert bocha.call_count == 1
    assert baidu.call_count == 1
    assert response.success is True
    assert response.provider == "Bocha+BaiduAI"
    assert [item.url for item in response.results] == [
        "https://www.cnstock.com/commonDetail/123",
        community_url,
    ]
    assert "主流财经媒体" in "".join(response.results[0].relevance_reasons or [])
    assert "社区讨论" in "".join(response.results[1].relevance_reasons or [])

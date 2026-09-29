from __future__ import annotations

import unittest
from collections.abc import Mapping

from enterprise_fabric_framework.connector import (
    ApiConnector,
    ApiResponse,
    ConnectionHealthStatus,
)


class _Transport:
    def __init__(self, responses: list[ApiResponse]) -> None:
        self._responses = responses
        self.requests: list[tuple[str, str, Mapping[str, str], bytes | None, float]] = []

    def __call__(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_seconds: float,
    ) -> ApiResponse:
        self.requests.append((method, url, headers, body, timeout_seconds))
        return self._responses.pop(0)


class ApiConnectorTests(unittest.TestCase):
    def test_new_session_sends_the_current_bearer_token(self) -> None:
        transport = _Transport([ApiResponse(200, {}, b"{}")])
        connector = ApiConnector(
            "https://api.example.test",
            access_token="initial-token",
            transport=transport,
        )

        response = connector.new_session().request("GET", "/records")

        self.assertEqual(response.status_code, 200)
        method, url, headers, _, _ = transport.requests[0]
        self.assertEqual(method, "GET")
        self.assertEqual(url, "https://api.example.test/records")
        self.assertEqual(headers["Authorization"], "Bearer initial-token")

    def test_renew_changes_tokens_for_future_sessions_only(self) -> None:
        transport = _Transport(
            [
                ApiResponse(200, {}, b"{}"),
                ApiResponse(200, {}, b"{}"),
            ]
        )
        connector = ApiConnector(
            "https://api.example.test",
            access_token="initial-token",
            token_provider=lambda: "renewed-token",
            transport=transport,
        )
        prior_session = connector.new_session()

        connector.renew()
        prior_session.request("GET", "/records")
        connector.new_session().request("GET", "/records")

        self.assertEqual(transport.requests[0][2]["Authorization"], "Bearer initial-token")
        self.assertEqual(transport.requests[1][2]["Authorization"], "Bearer renewed-token")

    def test_health_returns_a_bounded_http_failure(self) -> None:
        transport = _Transport([ApiResponse(503, {}, b"unavailable")])
        connector = ApiConnector("https://api.example.test", transport=transport)

        health = connector.health()

        self.assertEqual(health.status, ConnectionHealthStatus.UNHEALTHY)
        self.assertEqual(health.resource_ref, "/health")
        self.assertEqual(health.detail, "HTTP 503")

    def test_renew_requires_a_token_provider(self) -> None:
        connector = ApiConnector("https://api.example.test", access_token="initial-token")

        with self.assertRaisesRegex(RuntimeError, "no token_provider"):
            connector.renew()


if __name__ == "__main__":
    unittest.main()
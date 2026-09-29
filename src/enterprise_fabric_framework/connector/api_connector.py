"""HTTP API connector with explicit session and credential-renewal behavior."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TypeAlias
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from enterprise_fabric_framework.connector.connectors import (
    ConnectionHealth,
    ConnectionHealthStatus,
    Connector,
)

ApiTokenProvider: TypeAlias = Callable[[], str]
ApiTransport: TypeAlias = Callable[[str, str, Mapping[str, str], bytes | None, float], "ApiResponse"]


@dataclass(frozen=True, slots=True)
class ApiResponse:
    """Bounded HTTP response metadata and body returned by one API request."""

    status_code: int
    headers: Mapping[str, str]
    body: bytes


class ApiSession:
    """One API session with a snapshot of the connector's current access token."""

    def __init__(
        self,
        base_url: str,
        access_token: str | None,
        timeout_seconds: float,
        transport: ApiTransport,
    ) -> None:
        self._base_url = base_url
        self._access_token = access_token
        self._timeout_seconds = timeout_seconds
        self._transport = transport

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes | None = None,
    ) -> ApiResponse:
        """Send one request through this session without exposing its access token in results."""

        if not method.strip():
            raise ValueError("method must not be empty")
        if not path.startswith("/"):
            raise ValueError("path must begin with '/'")
        request_headers = {"Accept": "application/json"}
        if self._access_token is not None:
            request_headers["Authorization"] = f"Bearer {self._access_token}"
        if headers is not None:
            request_headers.update(headers)
        return self._transport(
            method.upper(),
            f"{self._base_url}{path}",
            request_headers,
            body,
            self._timeout_seconds,
        )


class ApiConnector(Connector):
    """Configured HTTP API access with connector-specific sessions and token renewal.

    Args:
        base_url: Root HTTPS or HTTP API URL, excluding any path selected by a request.
        access_token: Optional runtime-resolved bearer token. It is never returned in health data.
        token_provider: Optional callback that returns a replacement bearer token during ``renew``.
        health_path: Relative API path used by ``health``; it should not mutate remote state.
        timeout_seconds: Per-request network timeout.
        transport: Optional request function, mainly for custom transports and tests.
    """

    def __init__(
        self,
        base_url: str,
        *,
        access_token: str | None = None,
        token_provider: ApiTokenProvider | None = None,
        health_path: str = "/health",
        timeout_seconds: float = 30.0,
        transport: ApiTransport | None = None,
    ) -> None:
        """Create a connector for one HTTP API origin."""

        parsed_url = urlparse(base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("base_url must be an absolute HTTP or HTTPS URL")
        if access_token is not None and not access_token.strip():
            raise ValueError("access_token must not be empty when supplied")
        if not health_path.startswith("/"):
            raise ValueError("health_path must begin with '/'")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._base_url = base_url.rstrip("/")
        self._access_token = access_token
        self._token_provider = token_provider
        self._health_path = health_path
        self._timeout_seconds = timeout_seconds
        self._transport = transport or _urllib_transport

    def new_session(self) -> ApiSession:
        """Create an independent API session using the connector's current token snapshot."""

        return ApiSession(
            self._base_url,
            self._access_token,
            self._timeout_seconds,
            self._transport,
        )

    def renew(self) -> None:
        """Resolve and install a fresh bearer token using the configured provider."""

        if self._token_provider is None:
            raise RuntimeError("API connector has no token_provider for credential renewal")
        access_token = self._token_provider()
        if not access_token or not access_token.strip():
            raise ValueError("token_provider returned an empty access token")
        self._access_token = access_token

    def health(self) -> ConnectionHealth:
        """Call the configured safe endpoint and return bounded HTTP health evidence."""

        try:
            response = self.new_session().request("GET", self._health_path)
        except Exception as error:
            return ConnectionHealth(
                connector_type=type(self).__name__,
                status=ConnectionHealthStatus.UNHEALTHY,
                resource_ref=self._health_path,
                detail=type(error).__name__,
            )
        status = (
            ConnectionHealthStatus.HEALTHY
            if 200 <= response.status_code < 300
            else ConnectionHealthStatus.UNHEALTHY
        )
        return ConnectionHealth(
            connector_type=type(self).__name__,
            status=status,
            resource_ref=self._health_path,
            detail=None if status is ConnectionHealthStatus.HEALTHY else f"HTTP {response.status_code}",
        )


def _urllib_transport(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
    timeout_seconds: float,
) -> ApiResponse:
    request = Request(url=url, data=body, headers=dict(headers), method=method)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return ApiResponse(
                status_code=response.status,
                headers=dict(response.headers.items()),
                body=response.read(),
            )
    except HTTPError as error:
        return ApiResponse(
            status_code=error.code,
            headers=dict(error.headers.items()) if error.headers is not None else {},
            body=error.read(),
        )


__all__ = ["ApiConnector", "ApiResponse", "ApiSession", "ApiTokenProvider", "ApiTransport"]
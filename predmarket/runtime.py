"""Single ownership boundary for public, credential-free network resources."""

from __future__ import annotations

import httpx

from predmarket.polymarket.clob import ClobRestClient
from predmarket.polymarket.gamma import GammaClient


class Runtime:
    def __init__(
        self, *, http_transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.http = httpx.AsyncClient(
            transport=http_transport, timeout=10, trust_env=False
        )
        self.gamma = GammaClient(http=self.http)
        self.discovery_clob = ClobRestClient(http=self.http)
        self.confirmation_clob = ClobRestClient(http=self.http)
        self.fee_clob = ClobRestClient(http=self.http)
        self._entered = False

    async def __aenter__(self) -> "Runtime":
        if self._entered or self.http.is_closed:
            raise RuntimeError("runtime cannot be entered twice")
        self._entered = True
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.http.aclose()


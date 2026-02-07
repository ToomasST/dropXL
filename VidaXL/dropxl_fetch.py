#!/usr/bin/env python3
"""Minimal Prenta API client for the new PRENTA TOODETE TÕLKIMINE workflow.

This module provides only what Step 0 needs:
- ``ClientConfig`` dataclass with basic HTTP settings.
- ``PrentaClient`` with ``iter_categories()`` for fetching category data.

Authentication is via Basic Auth and is configured via ``ClientConfig``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import time
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter


DEFAULT_BASE_URL = "https://sandbox.prenta.lt/api/v1"


@dataclass
class ClientConfig:
    base_url: str = DEFAULT_BASE_URL
    username: str = ""
    password: str = ""
    timeout: int = 30
    max_retries: int = 5
    backoff_factor: float = 0.8
    per_page: int = 100  # API max
    verify_ssl: bool = True


class PrentaClient:
    """Minimal client wrapper used in the new workflow for category fetches.

    Only implements what Step 0 needs (paging + categories endpoint).
    """

    def __init__(self, cfg: ClientConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.password)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        # SSL verification can be toggled for sandbox issues
        self.session.verify = cfg.verify_ssl
        # Basic HTTP adapter with small connection pool
        self.session.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=8))
        self.session.mount("http://", HTTPAdapter(pool_connections=8, pool_maxsize=8))

    def _url(self, path: str) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self._url(path)
        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt < self.cfg.max_retries:
            attempt += 1
            try:
                resp = self.session.request(method, url, params=params, timeout=self.cfg.timeout)
                if resp.status_code == 401:
                    raise RuntimeError(
                        "401 Unauthorized: check PRENTA_USERNAME/PRENTA_PASSWORD or CLI flags"
                    )
                if resp.status_code >= 500 or resp.status_code in (429,):
                    raise RuntimeError(f"Transient HTTP {resp.status_code}")
                resp.raise_for_status()
                if not resp.content:
                    return None
                return resp.json()
            except Exception as e:
                last_exc = e
                if attempt >= self.cfg.max_retries:
                    break
                sleep_for = self.cfg.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_for)
        raise RuntimeError(f"Request failed for {url} params={params}: {last_exc}")

    def _paged(self, path: str, base_params: Optional[Dict[str, Any]] = None) -> Iterable[Any]:
        params = dict(base_params or {})
        page = 1
        per_page = min(max(1, self.cfg.per_page), 100)
        while True:
            params.update({"page": page, "per_page": per_page})
            data = self._request("GET", path, params=params)
            if data is None:
                break
            if not isinstance(data, list):
                if isinstance(data, dict):
                    items = data.get("items") or data.get("results") or []
                else:
                    items = []
            else:
                items = data
            if not items:
                break
            for item in items:
                yield item
            if len(items) < per_page:
                break
            page += 1

    # Public endpoint used in Step 0
    def iter_categories(self) -> Iterable[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        return self._paged("categories", params)

    # Additional endpoints used in Step 1

    def iter_products(self, list_limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        """Iterate over products list from /products endpoint.

        list_limit (if > 0) piirab maksimaalset tagastatavate toodete arvu
        (kasulik testimiseks). Kui None või 0, loetakse kõik lehed.
        """

        count = 0
        for item in self._paged("products", {}):
            yield item
            count += 1
            if list_limit and list_limit > 0 and count >= list_limit:
                break

    def get_product_detail(self, product_id: Any) -> Dict[str, Any]:
        """Fetch a single product detail via /products/{product_id}."""

        return self._request("GET", f"products/{product_id}", params=None)

    def iter_product_attributes(self, product_id: Any) -> Iterable[Dict[str, Any]]:
        """Iterate over attribute records for a given product.

        Wraps GET /products/{product_id}/attributes.
        """

        params: Dict[str, Any] = {}
        return self._paged(f"products/{product_id}/attributes", params)

    def iter_product_attribute_values(self, product_id: Any) -> Iterable[Dict[str, Any]]:
        """Iterate over attribute_value records for a given product.

        Wraps GET /products/{product_id}/attribute_values.
        """

        params: Dict[str, Any] = {}
        return self._paged(f"products/{product_id}/attribute_values", params)

    def iter_prices(self, product_id: Optional[Any] = None) -> Iterable[Dict[str, Any]]:
        """Iterate over price records.

        Wraps GET /prices. Kui product_id on antud, kasutatakse seda filtrina.
        """

        params: Dict[str, Any] = {"order": "product_id"}
        if product_id is not None:
            params["product_id"] = product_id
        return self._paged("prices", params)

    def iter_stock_levels(self, product_id: Optional[Any] = None) -> Iterable[Dict[str, Any]]:
        """Iterate over stock level records.

        Wraps GET /stock_levels. Kui product_id on antud, kasutatakse seda filtrina.
        """

        params: Dict[str, Any] = {"order": "product_id"}
        if product_id is not None:
            params["product_id"] = product_id
        return self._paged("stock_levels", params)

    def iter_manufacturers(self) -> Iterable[Dict[str, Any]]:
        """Iterate over manufacturers list via /manufacturers endpoint."""

        params: Dict[str, Any] = {}
        return self._paged("manufacturers", params)

    def iter_uoms(self) -> Iterable[Dict[str, Any]]:
        """Iterate over Units of Measurement via /uoms endpoint."""

        params: Dict[str, Any] = {}
        return self._paged("uoms", params)


class DropXLClient:
    """Minimal DropXL API client for /api_customer/products.

    DropXL API uses offset/limit pagination and basic auth.
    """

    def __init__(self, cfg: ClientConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.password)
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self.session.verify = cfg.verify_ssl
        self.session.mount("https://", HTTPAdapter(pool_connections=8, pool_maxsize=8))
        self.session.mount("http://", HTTPAdapter(pool_connections=8, pool_maxsize=8))

    def _url(self, path: str) -> str:
        return f"{self.cfg.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = self._url(path)
        attempt = 0
        last_exc: Optional[Exception] = None
        while attempt < self.cfg.max_retries:
            attempt += 1
            try:
                resp = self.session.request(method, url, params=params, timeout=self.cfg.timeout)
                if resp.status_code == 401:
                    raise RuntimeError(
                        "401 Unauthorized: check DROPXL_USERNAME/DROPXL_PASSWORD or CLI flags"
                    )
                if resp.status_code >= 500 or resp.status_code in (429,):
                    raise RuntimeError(f"Transient HTTP {resp.status_code}")
                resp.raise_for_status()
                if not resp.content:
                    return None
                return resp.json()
            except Exception as e:
                last_exc = e
                if attempt >= self.cfg.max_retries:
                    break
                sleep_for = self.cfg.backoff_factor * (2 ** (attempt - 1))
                time.sleep(sleep_for)
        raise RuntimeError(f"Request failed for {url} params={params}: {last_exc}")

    def iter_products(self, list_limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        """Iterate over products list from /api_customer/products.

        Uses offset/limit pagination; API limit is 500 per request.
        """

        limit = min(max(1, self.cfg.per_page), 500)
        offset = 0
        count = 0
        while True:
            params = {"limit": limit, "offset": offset}
            data = self._request("GET", "api_customer/products", params=params)
            if not data:
                break
            items = data
            if isinstance(data, dict):
                items = data.get("data") or []
            if not isinstance(items, list):
                break
            for item in items:
                if isinstance(item, dict):
                    yield item
                    count += 1
                    if list_limit and list_limit > 0 and count >= list_limit:
                        return
            if len(items) < limit:
                break
            offset += limit
            time.sleep(1.05)

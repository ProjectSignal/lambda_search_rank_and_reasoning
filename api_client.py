"""API helper functions for the RankAndReasoning Lambda."""

from typing import Any, Dict, Iterable, Optional, Sequence

import requests

from config import DATA_API_BASE_URL, DATA_API_KEY, DATA_API_TIMEOUT
from logging_config import setup_logger

logger = setup_logger(__name__)


class SearchServiceError(RuntimeError):
    """Raised when the upstream search data service fails."""


def _headers() -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if DATA_API_KEY:
        headers["x-api-key"] = DATA_API_KEY
    return headers


def _extract_payload(response: requests.Response) -> Any:
    try:
        payload = response.json()
    except ValueError:  # pragma: no cover
        payload = {}
    if isinstance(payload, dict) and "data" in payload:
        return payload["data"]
    return payload


def _user_params(user_id: str) -> Dict[str, str]:
    if not user_id:
        raise ValueError("user_id is required for search API calls")
    return {"userId": str(user_id)}


def get_search_document(search_id: str, *, user_id: str) -> Optional[Dict[str, Any]]:
    """
    Load the search document referenced by ``search_id``.

    Input: identifier string. Output: dict when found, otherwise ``None``.
    """
    url = f"{DATA_API_BASE_URL}/search/{search_id}"
    try:
        response = requests.get(
            url,
            headers=_headers(),
            params=_user_params(user_id),
            timeout=DATA_API_TIMEOUT,
        )
    except requests.RequestException as exc:  # pragma: no cover
        raise SearchServiceError(f"Failed to fetch search {search_id}: {exc}") from exc

    if response.status_code == 404:
        return None
    if not response.ok:
        raise SearchServiceError(
            f"Search API returned {response.status_code} while fetching {search_id}: {response.text}"
        )
    return _extract_payload(response)


def update_search_document(
    search_id: str,
    *,
    user_id: str,
    set_fields: Optional[Dict[str, Any]] = None,
    append_events: Optional[Sequence[Dict[str, Any]]] = None,
    expected_statuses: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Persist partial updates to the search document.

    Input parameters mirror the optimistic updates we previously performed via MongoDB.
    Output is the updated document snapshot returned by the API.
    """
    payload: Dict[str, Any] = {"userId": str(user_id)}
    if set_fields:
        payload["set"] = set_fields
    if append_events:
        payload["appendEvents"] = list(append_events)
    if expected_statuses:
        payload["expectedStatus"] = list(expected_statuses)

    url = f"{DATA_API_BASE_URL}/search/{search_id}"
    try:
        response = requests.patch(
            url,
            json=payload,
            headers=_headers(),
            timeout=DATA_API_TIMEOUT,
        )
    except requests.RequestException as exc:  # pragma: no cover
        raise SearchServiceError(f"Failed to update search {search_id}: {exc}") from exc

    if not response.ok:
        raise SearchServiceError(
            f"Search API returned {response.status_code} while updating {search_id}: {response.text}"
        )

    return _extract_payload(response)


def delete_search_document(search_id: str, *, user_id: str) -> None:
    """Delete a search document for cleanup routines."""
    url = f"{DATA_API_BASE_URL}/search/{search_id}"
    try:
        response = requests.delete(
            url,
            headers=_headers(),
            params=_user_params(user_id),
            timeout=DATA_API_TIMEOUT,
        )
    except requests.RequestException as exc:  # pragma: no cover
        raise SearchServiceError(f"Failed to delete search document {search_id}: {exc}") from exc

    if response.status_code in (200, 202, 204, 404):
        return
    raise SearchServiceError(
        f"Search API returned {response.status_code} while deleting {search_id}: {response.text}"
    )


def fetch_nodes_by_ids(
    node_ids: Iterable[str],
    *,
    projection: Optional[Dict[str, int]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Bulk-load node documents for the provided identifiers.

    Input: iterable of nodeId strings plus optional projection dict.
    Output: dict mapping nodeId -> document payload returned by the API.
    """
    ids = [str(node_id) for node_id in node_ids if node_id]
    if not ids:
        return {}

    payload: Dict[str, Any] = {"ids": ids}
    if projection:
        payload["projection"] = projection

    url = f"{DATA_API_BASE_URL}/nodes/bulk"
    try:
        response = requests.post(
            url,
            json=payload,
            headers=_headers(),
            timeout=DATA_API_TIMEOUT,
        )
    except requests.RequestException as exc:  # pragma: no cover
        raise SearchServiceError(f"Failed to bulk fetch nodes: {exc}") from exc

    if not response.ok:
        raise SearchServiceError(
            f"Node bulk fetch failed with status {response.status_code}: {response.text}"
        )

    data = _extract_payload(response)
    if isinstance(data, list):
        return {doc.get("_id") or doc.get("nodeId"): doc for doc in data}
    if isinstance(data, dict):
        return data
    logger.warning("Unexpected payload when fetching nodes: %s", data)
    return {}


def get_node_document(node_id: str, *, projection: Optional[Dict[str, int]] = None) -> Optional[Dict[str, Any]]:
    """
    Convenience helper to resolve a single node document.

    Input: ``node_id`` string and optional projection dict.
    Output: Node document dict or ``None`` on 404.
    """
    url = f"{DATA_API_BASE_URL}/nodes/get"
    payload: Dict[str, Any] = {"id": str(node_id)}
    if projection:
        payload["projection"] = projection

    try:
        response = requests.post(
            url,
            json=payload,
            headers=_headers(),
            timeout=DATA_API_TIMEOUT,
        )
    except requests.RequestException as exc:  # pragma: no cover
        raise SearchServiceError(f"Failed to fetch node {node_id}: {exc}") from exc

    if response.status_code == 404:
        return None
    if not response.ok:
        raise SearchServiceError(
            f"Node fetch failed with status {response.status_code}: {response.text}"
        )

    return _extract_payload(response)

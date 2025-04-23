from fastapi import Request, HTTPException
from typing import Any, Callable, Dict, Tuple, List, Optional
import httpx


def extract_and_validate_headers(request: Request) -> Dict[str, str]:
    headers = dict(request.headers)
    headers.pop("host", None)
    if "authorization" not in {k.lower() for k in headers}:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    return headers


def build_params(**kwargs) -> Dict[str, Any]:
    return {k: v for k, v in kwargs.items() if v is not None}


async def paginate_all_data(
    fetch_page: Callable[[int], Any],
    limit: int,
    offset: int,
    total_count_key: str,
    data_extractor: Callable[[dict], List[Any]]
) -> Tuple[dict, List[Any], dict]:
    """
    Generic pagination helper for APIs.
    fetch_page: async function to fetch a page given an offset
    limit: page size
    offset: starting offset
    total_count_key: key in meta for total count
    data_extractor: function to extract the list of items from the response payload
    Returns: (meta, all_items, extra_info)
    """
    all_items = []
    current_offset = offset
    total_count = None
    meta = None
    extra_info = {}
    while True:
        resp = await fetch_page(current_offset)
        if resp.status_code != 200:
            break
        payload = resp.json()
        if meta is None:
            meta = payload.get("meta", {})
            total_count = meta.get(total_count_key)
            # For zone-and-port-traffic, capture zone_port_information if present
            if "zone_port_information" in payload.get("data", {}):
                extra_info["zone_port_information"] = payload["data"]["zone_port_information"]
        items = data_extractor(payload)
        if not isinstance(items, list):
            items = []
        all_items.extend(items)
        current_offset += limit
        if not items or (total_count is not None and current_offset >= total_count):
            break
    return meta, all_items, extra_info

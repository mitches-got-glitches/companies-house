"""Provides an asynchronous fetch function for the Companies House API.

Can work as a basic template for any API using the `httpx` library. For a list of
endpoints and parameters see the API reference:
https://developer-specs.company-information.service.gov.uk/companies-house-public-data-api/reference
"""

import httpx

from companies_house._typing import JSON
from companies_house.config import settings
from companies_house.constants import BASE_URL


async def fetch_data(endpoint: str, params: dict[str | int] | None = None) -> JSON:
    """Fetch data from the Companies House API for a given endpoint.

    Uses the BASE_URL set in constants and the API_KEY which is provided as a secret.

    Args:
        endpoint: The target endpoint (see API docs for a list of endpoints).
        params: The request parameters to feed into the request.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/{endpoint}",
            auth=(settings.API_KEY, ""),
            params=params,
        )
        response.raise_for_status()
        return response.json()

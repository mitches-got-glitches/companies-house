"""Provide functions to request data from the Companies House API.

Companies House API overview: https://developer.company-information.service.gov.uk/
You can make up to 600 requests within a 5 minute period.
"""

import asyncio

import polars as pl

from companies_house._typing import JSON
from companies_house.companies_house_api import fetch_data


async def get_company_profile(company_number: str) -> JSON:
    """Fetch data for the given company number from the "company" endpoint.

    Returns:
        The JSON response from the endpoint.
    """
    endpoint = f"company/{company_number}"
    return await fetch_data(endpoint)


async def get_company_profile_as_df(company_number: str) -> pl.DataFrame:
    """Fetch data for the given company number and wrap in polars DataFrame.

    Returns:
        A polars DataFrame created from the JSON response.
    """
    return pl.DataFrame(await get_company_profile(company_number))


async def advanced_search(**params) -> JSON:
    """Fetch company summary data from the advanced search endpoint.

    Args:
        params: For list of appropriate kwargs, see API overview in module docstring.

    Returns:
        The JSON response from the endpoint.
    """
    endpoint = "advanced-search/companies"
    return await fetch_data(endpoint, params=params)


async def advanced_search_as_df(**params) -> pl.DataFrame:
    """Fetch results from the advanced search and wrap in polars DataFrame.

    Args:
        params: For list of appropriate kwargs, see API overview in module docstring.

    Returns:
        A polars DataFrame created from the JSON response.
    """
    data = await advanced_search(params)
    return pl.DataFrame(data["items"])


if __name__ == "__main__":
    # Test the endpoint with an example if run as main.
    company_number = "00000006"
    df_comapny = asyncio.run(get_company_profile_as_df(company_number))
    print(df_comapny)

    df_advanced_search = asyncio.run(
        advanced_search_as_df(
            incorporated_from="2024-05-06",
            incorporated_to="2024-06-05",
        )
    )
    print(df_advanced_search)

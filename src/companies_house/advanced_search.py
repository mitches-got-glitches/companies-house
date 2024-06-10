"""Provide an advanced search"""

import asyncio

import httpx
import polars as pl

from companies_house.companies_house_api import fetch_data


async def get_advanced_search(params: dict[str | int] = None) -> dict:
    endpoint = "advanced-search/companies"
    data = await fetch_data(endpoint, params=params)
    return data


# Main function to demonstrate the workflow
async def main(**params) -> pl.DataFrame:
    data = await get_advanced_search(params)
    return pl.DataFrame(data["items"])


if __name__ == "__main__":
    incorporated_from = "2024-05-06"
    incorporated_to = "2024-06-05"

    df = asyncio.run(main(incorporated_from, incorporated_to))
    print(df)

import asyncio

import httpx
import polars as pl

from .companies_house_api import fetch_data


async def get_advanced_search(params: dict[str | int] = None) -> dict:
    endpoint = "advanced-search/companies"
    data = await fetch_data(endpoint, params=params)
    return data


# Main function to demonstrate the workflow
async def main(**params) -> pl.DataFrame:
    try:
        data = await get_advanced_search(params)
        return pl.DataFrame(data["items"])

    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    incorporated_from = "2024-05-06"
    incorporated_to = "2024-06-05"

    df = asyncio.run(main(incorporated_from, incorporated_to))
    print(df)

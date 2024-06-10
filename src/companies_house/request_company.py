import asyncio

import httpx
import polars as pl

from .companies_house_api import fetch_data


async def get_company_profile(company_number: str) -> dict:
    endpoint = f"company/{company_number}"
    data = await fetch_data(endpoint)
    return data


async def get_company_profile_as_df(company_number: str) -> pl.DataFrame:
    try:
        data = await get_company_profile(company_number)
        return pl.DataFrame(data)
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    company_number = "00000006"
    df = asyncio.run(get_company_profile_as_df(company_number))
    print(df)

import httpx

from .config import settings

# Replace 'your_api_key_here' with your actual Companies House API key
API_KEY = settings.COMPANIES_HOUSE_API_KEY
BASE_URL = "https://api.company-information.service.gov.uk"


# Function to fetch data from the Companies House API
async def fetch_data(endpoint: str, params: dict[str | int] | None = None):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{BASE_URL}/{endpoint}",
            auth=(API_KEY, ""),
            params=params,
        )
        response.raise_for_status()
        return response.json()

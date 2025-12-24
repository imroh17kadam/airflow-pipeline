import requests
import logging

def extract_data(api_url: str) -> list:
    logging.info("Starting data extraction")

    response = requests.get(api_url, timeout=10)

    if response.status_code != 200:
        raise Exception(f"API call failed: {response.status_code}")

    data = response.json()

    if not data:
        raise Exception("No data received from API")

    logging.info(f"Extracted {len(data)} records")
    return data
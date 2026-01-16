import requests
from typing import Optional

import monday_config


def post(json_body: str) -> str:
    """
    Make a POST request to Monday.com API
    """
    headers = {
        "Authorization": monday_config.MONDAY_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        monday_config.MONDAY_API_URL,
        headers=headers,
        data=json_body
    )
    
    if not response.ok:
        error_body = response.text if response.text else "No error body"
        raise IOError(f"HTTP error {response.status_code}: {error_body}")
    
    return response.text


def download_file(url: str) -> bytes:
    """
    Download file from public URL (no auth required)
    """
    response = requests.get(url)
    
    if not response.ok:
        raise IOError(f"Download failed: {response.status_code} for URL: {url}")
    
    return response.content


def download_file_with_auth(url: str) -> bytes:
    """
    Download file from Monday.com API (auth required)
    """
    headers = {
        "Authorization": monday_config.MONDAY_API_TOKEN
    }
    
    response = requests.get(url, headers=headers)
    
    if not response.ok:
        error_body = response.text if response.text else "No error body"
        raise IOError(f"Download failed: {response.status_code} for URL: {url}. Error: {error_body}")
    
    return response.content

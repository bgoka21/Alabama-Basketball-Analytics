"""
Synergy API Authentication Handler
"""
import time
import requests

_token_cache = {
    "access_token": None,
    "expires_at": 0
}

# These will be set by the routes module
SYNERGY_CLIENT_ID = None
SYNERGY_CLIENT_SECRET = None
SYNERGY_SCOPE = "api.basketball.external"
SYNERGY_TOKEN_URL = "https://auth.synergysportstech.com/connect/token"


def get_synergy_token():
    """
    Returns a valid Synergy access token.
    Automatically refreshes if expired.
    """
    now = time.time()

    # If we already have a valid token, reuse it
    if _token_cache["access_token"] and now < _token_cache["expires_at"]:
        return _token_cache["access_token"]

    payload = {
        "grant_type": "client_credentials",
        "client_id": SYNERGY_CLIENT_ID,
        "client_secret": SYNERGY_CLIENT_SECRET,
        "scope": SYNERGY_SCOPE
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    print("=" * 60)
    print("DEBUG: Synergy Auth Request")
    print(f"URL: {SYNERGY_TOKEN_URL}")
    print(f"Client ID: {SYNERGY_CLIENT_ID}")
    print(f"Client Secret: {SYNERGY_CLIENT_SECRET[:10]}..." if SYNERGY_CLIENT_SECRET else "None")
    print(f"Scope: {SYNERGY_SCOPE}")
    print(f"Payload: {payload}")
    print("=" * 60)

    response = requests.post(SYNERGY_TOKEN_URL, data=payload, headers=headers)
    response.raise_for_status()

    data = response.json()

    _token_cache["access_token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 3600) - 60

    return _token_cache["access_token"]
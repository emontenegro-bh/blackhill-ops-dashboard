#!/usr/bin/env python3
"""
Aspire API Authentication Script
Handles JWT token acquisition via ClientId/Secret, caching, and validation.

Supports two API clients:
  - Lead Monitor: Contacts only (GET/POST/PUT)
  - Reporting: All 101 controllers, GET-only

Usage:
    python3 aspire-auth.py              # Get token (Reporting client, default)
    python3 aspire-auth.py --client lead  # Get token (Lead Monitor client)
    python3 aspire-auth.py --test       # Test connection
    python3 aspire-auth.py --status     # Show token status

Config: ~/.config/aspire/config.json
Token cache: ~/.config/aspire/reporting-token.json or api-token.json
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

CONFIG_DIR = Path.home() / ".config" / "aspire"
CONFIG_FILE = CONFIG_DIR / "config.json"

# Token cache files per client
TOKEN_FILES = {
    "reporting": CONFIG_DIR / "reporting-token.json",
    "lead": CONFIG_DIR / "api-token.json",
}

# Token refresh buffer — refresh 5 minutes before expiry
REFRESH_BUFFER_SECONDS = 300


def load_config(client="reporting"):
    """Load API config from ~/.config/aspire/config.json.

    Args:
        client: 'reporting' (default, broad read access) or 'lead' (contacts only)

    Returns dict with api_base_url, client_id, secret.
    """
    # Try env vars first (for CI/cloud)
    if client == "reporting":
        env_id = os.environ.get("ASPIRE_REPORTING_CLIENT_ID")
        env_secret = os.environ.get("ASPIRE_REPORTING_SECRET")
    else:
        env_id = os.environ.get("ASPIRE_CLIENT_ID")
        env_secret = os.environ.get("ASPIRE_SECRET")

    if env_id and env_secret:
        base = os.environ.get("ASPIRE_API_URL", "https://cloud-api.youraspire.com")
        return {"api_base_url": base, "client_id": env_id, "secret": env_secret}

    # Load from config file
    if not CONFIG_FILE.exists():
        print(f"ERROR: Config not found at {CONFIG_FILE}", file=sys.stderr)
        sys.exit(1)

    with open(CONFIG_FILE) as f:
        cfg = json.load(f)

    base_url = cfg.get("api_base_url", "https://cloud-api.youraspire.com")

    if client == "reporting":
        client_id = cfg.get("reporting_client_id")
        secret = cfg.get("reporting_secret")
    else:
        client_id = cfg.get("api_client_id")
        secret = cfg.get("api_secret")

    if not client_id or not secret:
        print(f"ERROR: No {client} API client credentials in config", file=sys.stderr)
        sys.exit(1)

    return {"api_base_url": base_url, "client_id": client_id, "secret": secret}


def load_cached_token(client="reporting"):
    """Load cached token if it exists and is still valid."""
    token_file = TOKEN_FILES.get(client, TOKEN_FILES["reporting"])
    if not token_file.exists():
        return None

    try:
        with open(token_file) as f:
            cached = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    token = cached.get("token", "")
    if not token:
        return None

    # Check expiry if we have it
    expires_at = cached.get("expires_at", 0)
    if expires_at and time.time() > (expires_at - REFRESH_BUFFER_SECONDS):
        return None

    return cached


def save_token(token_data, client="reporting"):
    """Save token to cache file with restricted permissions."""
    token_file = TOKEN_FILES.get(client, TOKEN_FILES["reporting"])
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(token_file, "w") as f:
        json.dump(token_data, f, indent=2)
    os.chmod(token_file, 0o600)


def authenticate(config, client="reporting"):
    """Get a new JWT token via POST /Authorization with ClientId + Secret."""
    base = config["api_base_url"].rstrip("/")
    url = f"{base}/Authorization"

    payload = json.dumps({
        "ClientId": config["client_id"],
        "Secret": config["secret"],
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        err = e.read().decode() if e.fp else str(e)
        print(f"ERROR: Authentication failed (HTTP {e.code}): {err}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    token = data.get("Token", "")
    refresh = data.get("RefreshToken", "")

    if not token:
        print(f"ERROR: No token in response. Keys: {list(data.keys())}", file=sys.stderr)
        sys.exit(1)

    token_data = {
        "token": token,
        "refresh_token": refresh,
        "expires_at": time.time() + 3600,  # Default 1 hour
        "obtained_at": datetime.now(timezone.utc).isoformat(),
        "client": client,
    }

    save_token(token_data, client)
    return token_data


def validate_token(config, token):
    """Quick check if a token is still valid by hitting a lightweight endpoint."""
    base = config["api_base_url"].rstrip("/")
    req = urllib.request.Request(
        f"{base}/ContactTypes?$top=1",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:
        return False


def get_token(client="reporting"):
    """Get a valid token — from cache or new auth.

    This is the main entry point for other scripts to import and use.
    """
    config = load_config(client)

    # Try cached token
    cached = load_cached_token(client)
    if cached and cached.get("token"):
        # Optionally validate
        if validate_token(config, cached["token"]):
            return cached["token"]

    # Full authentication
    token_data = authenticate(config, client)
    return token_data["token"]


def test_connection(client="reporting"):
    """Test the API connection."""
    config = load_config(client)
    base = config["api_base_url"].rstrip("/")

    print(f"Testing Aspire API ({client} client)...")
    print(f"  Base URL: {base}")

    # Test auth
    print("\nAuthenticating...")
    try:
        token_data = authenticate(config, client)
        token = token_data["token"]
        print(f"  Token obtained: {token[:20]}...")
    except SystemExit:
        return False

    # Test an endpoint
    print("\nTesting API access...")
    test_endpoints = ["ContactTypes", "Branches", "Divisions"]
    for ep in test_endpoints:
        req = urllib.request.Request(
            f"{base}/{ep}?$top=3",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                count = len(data) if isinstance(data, list) else "N/A"
                print(f"  {ep}: OK ({count} records)")
        except urllib.error.HTTPError as e:
            print(f"  {ep}: HTTP {e.code} (access denied or not available)")
        except Exception as e:
            print(f"  {ep}: Error ({e})")

    # Test Opportunities (only Reporting client has access)
    if client == "reporting":
        req = urllib.request.Request(
            f"{base}/Opportunities?$top=1",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
                print(f"  Opportunities: OK ({len(data)} records)")
        except urllib.error.HTTPError as e:
            print(f"  Opportunities: HTTP {e.code}")

    print("\nConnection test complete.")
    return True


def show_status(client="reporting"):
    """Show current token status."""
    token_file = TOKEN_FILES.get(client, TOKEN_FILES["reporting"])
    if not token_file.exists():
        print(f"No cached token found for {client} client.")
        return

    with open(token_file) as f:
        cached = json.load(f)

    obtained = cached.get("obtained_at", "unknown")
    expires_at = cached.get("expires_at", 0)
    remaining = expires_at - time.time()

    print(f"Client: {cached.get('client', client)}")
    print(f"Token obtained: {obtained}")

    if remaining > 0:
        mins = int(remaining // 60)
        print(f"Expires in: {mins} minutes")
        print("Status: VALID")
    else:
        print(f"Expired: {int(-remaining // 60)} minutes ago")
        print("Status: EXPIRED (will auto-refresh on next use)")


def main():
    parser = argparse.ArgumentParser(description="Aspire API Authentication")
    parser.add_argument("--test", action="store_true", help="Test API connection")
    parser.add_argument("--status", action="store_true", help="Show token status")
    parser.add_argument("--client", choices=["reporting", "lead"], default="reporting",
                        help="API client to use (default: reporting)")
    args = parser.parse_args()

    if args.test:
        success = test_connection(args.client)
        sys.exit(0 if success else 1)
    elif args.status:
        show_status(args.client)
    else:
        token = get_token(args.client)
        print(token)


if __name__ == "__main__":
    main()

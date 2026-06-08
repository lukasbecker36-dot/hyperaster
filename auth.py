"""
API authentication for AsterDEX and Hyperliquid.

AsterDEX: EIP-712 structured data signing with Ethereum private key (V3 API)
Hyperliquid: EIP-712 typed signing (L1 actions) + info endpoints (unsigned)

Keys are loaded from environment variables (never stored in code).
"""

import hashlib
import logging
import os
import time
import threading

from eth_account import Account
from eth_account.messages import encode_typed_data

log = logging.getLogger(__name__)


def load_api_keys() -> dict:
    """
    Load API keys from environment variables.
    Returns dict with keys for both exchanges.
    Raises EnvironmentError if any required key is missing.
    """
    required = {
        "aster_api_key": "ASTER_API_KEY",           # agent wallet address (0x...)
        "aster_api_secret": "ASTER_API_SECRET",      # agent private key (0x...)
        "aster_wallet_address": "ASTER_WALLET_ADDRESS",  # main wallet / user (0x...)
        "hl_private_key": "HL_PRIVATE_KEY",          # Hyperliquid wallet private key (0x...)
        "hl_wallet_address": "HL_WALLET_ADDRESS",    # Hyperliquid wallet address (0x...)
    }

    keys = {}
    missing = []
    for name, env_var in required.items():
        val = os.environ.get(env_var)
        if not val:
            missing.append(env_var)
        keys[name] = val or ""

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them before running the live monitor."
        )

    # Derive the signer address from the Aster private key
    acct = Account.from_key(keys["aster_api_secret"])
    keys["aster_signer_address"] = acct.address
    if keys["aster_api_key"].lower() != acct.address.lower():
        log.warning(
            f"ASTER_API_KEY ({keys['aster_api_key']}) does not match "
            f"signer derived from ASTER_API_SECRET ({acct.address})"
        )
    log.info(f"AsterDEX signer address: {acct.address}")

    # Derive Hyperliquid address
    hl_acct = Account.from_key(keys["hl_private_key"])
    if keys["hl_wallet_address"].lower() != hl_acct.address.lower():
        log.warning(
            f"HL_WALLET_ADDRESS ({keys['hl_wallet_address']}) does not match "
            f"derived from HL_PRIVATE_KEY ({hl_acct.address})"
        )
    log.info(f"Hyperliquid wallet address: {hl_acct.address}")

    log.info("API keys loaded from environment variables")
    return keys


def now_ms() -> int:
    """Current time in milliseconds."""
    return int(time.time() * 1000)


# ── AsterDEX V3 EIP-712 Authentication ──

_nonce_lock = threading.Lock()
_nonce_counter = 0
_nonce_last_ms = 0


def _generate_nonce() -> int:
    """
    Generate a unique nonce per official AsterDEX demo:
    seconds-since-epoch * 1_000_000 + monotonic counter (per-second).
    """
    global _nonce_counter, _nonce_last_ms
    with _nonce_lock:
        current_s = int(time.time())
        if current_s == _nonce_last_ms:
            _nonce_counter += 1
        else:
            _nonce_counter = 0
            _nonce_last_ms = current_s
        return current_s * 1_000_000 + _nonce_counter


ASTER_EIP712_DOMAIN = {
    "name": "AsterSignTransaction",
    "version": "1",
    "chainId": 1666,
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}


def sign_aster_request(
    params: dict,
    private_key: str,
    user_address: str,
    signer_address: str,
) -> dict:
    """
    Sign an AsterDEX V3 API request using EIP-712 (agent-signed scheme).

    Process:
    1. Append asterChain, user, signer, nonce to params in INSERTION order
    2. Build message string: "k=v&k=v..." in insertion order (NOT sorted!)
    3. EIP-712 sign with agent private key
    4. Append signature to params
    """
    params["asterChain"] = "Mainnet"
    params["user"] = user_address
    params["signer"] = signer_address
    params["nonce"] = _generate_nonce()

    msg = "&".join(f"{k}={v}" for k, v in params.items())

    typed_data = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Message": [{"name": "msg", "type": "string"}],
        },
        "primaryType": "Message",
        "domain": ASTER_EIP712_DOMAIN,
        "message": {"msg": msg},
    }

    signable = encode_typed_data(full_message=typed_data)
    signed = Account.sign_message(signable, private_key=private_key)

    params["signature"] = signed.signature.hex()
    return params

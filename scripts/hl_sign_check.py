#!/usr/bin/env python3
"""
Offline HL signing self-check. Places NO order and hits NO network.

Loads the live keys, signs a sample HL `Alo` order action exactly the way
ExchangeClient does, then recovers the signer address from the produced
signature. If the recovered address != the address derived from
HL_PRIVATE_KEY, our signature is malformed (Hyperliquid would reject it as
"User or API Wallet 0x... does not exist", recovering a phantom address).

Run on the server with the project venv:
    .venv/bin/python scripts/hl_sign_check.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(os.path.join(Path(__file__).parent.parent, ".env"))

import msgpack
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak
import eth_account, eth_utils

from auth import load_api_keys


def sign_like_client(action: dict, nonce: int, priv: str):
    """Mirror ExchangeClient._hl_sign_action exactly."""
    data = msgpack.packb(action)
    data += nonce.to_bytes(8, "big")
    data += b"\x00"  # vault_address is None for orders
    connection_id = keccak(primitive=data)
    typed_data = {
        "domain": {
            "chainId": 1337, "name": "Exchange",
            "verifyingContract": "0x0000000000000000000000000000000000000000",
            "version": "1",
        },
        "types": {
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
        },
        "primaryType": "Agent",
        "message": {"source": "a", "connectionId": connection_id},
    }
    signable = encode_typed_data(full_message=typed_data)
    signed = Account.sign_message(signable, private_key=priv)
    payload_sig = {
        "r": "0x" + format(signed.r, "064x"),
        "s": "0x" + format(signed.s, "064x"),
        "v": signed.v,
    }
    return signable, signed, payload_sig


def main():
    print(f"eth_account={eth_account.__version__}  eth_utils={eth_utils.__version__}")
    keys = load_api_keys()
    priv = keys["hl_private_key"]
    derived = Account.from_key(priv).address
    print(f"HL_PRIVATE_KEY derives: {derived}")
    print(f"HL_WALLET_ADDRESS env:  {keys['hl_wallet_address']}")

    order = {"a": 110061, "b": False, "p": "245.50", "s": "0.40",
             "r": False, "t": {"limit": {"tif": "Alo"}}}
    action = {"type": "order", "orders": [order], "grouping": "na"}
    nonce = 1781650000000

    signable, signed, payload_sig = sign_like_client(action, nonce, priv)
    print(f"signed.v = {signed.v}  (HL expects 27 or 28)")

    rec = Account.recover_message(
        signable, vrs=(payload_sig["v"], signed.r, signed.s)
    )
    ok = rec.lower() == derived.lower()
    print(f"recovered from OUR signature: {rec}")
    print("RESULT:", "OK — signature is valid" if ok
          else f"*** PHANTOM *** HL would see {rec}, not {derived}")

    if not ok:
        # Show what the corrected (27/28-normalised) v would recover.
        v_norm = payload_sig["v"]
        v_norm = v_norm + 27 if v_norm in (0, 1) else v_norm
        rec2 = Account.recover_message(signable, vrs=(v_norm, signed.r, signed.s))
        print(f"with v normalised to {v_norm}: recovers {rec2} "
              f"({'OK' if rec2.lower()==derived.lower() else 'still wrong'})")


if __name__ == "__main__":
    main()

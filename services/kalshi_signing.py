import base64
import time
from typing import Optional, Dict


def build_auth_headers(
    key_id: str,
    private_key_path: str,
    method: str,
    path: str,
    timestamp_ms: Optional[str] = None,
) -> Dict[str, str]:
    if not timestamp_ms:
        timestamp_ms = str(int(time.time() * 1000))

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
    except Exception as e:
        raise ImportError("cryptography is required for Kalshi request signing") from e

    with open(private_key_path, "rb") as f:
        key_data = f.read()

    private_key = serialization.load_pem_private_key(key_data, password=None)
    message = f"{timestamp_ms}{method.upper()}{path}"

    signature = private_key.sign(
        message.encode("utf-8"),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    signature_b64 = base64.b64encode(signature).decode("utf-8")

    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": signature_b64,
    }

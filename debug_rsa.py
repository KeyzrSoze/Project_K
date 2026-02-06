import inspect
from kalshi_python import ApiClient

print("--- DIAGNOSTIC: ApiClient.set_kalshi_auth Signature ---")
try:
    # We inspect the signature to see the EXACT argument names
    sig = inspect.signature(ApiClient.set_kalshi_auth)
    print(str(sig))
except Exception as e:
    print(f"Could not inspect signature: {e}")

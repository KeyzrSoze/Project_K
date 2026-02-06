import inspect
from kalshi_python import ApiClient

print("--- DIAGNOSTIC: ApiClient.call_api Signature ---")
try:
    sig = inspect.signature(ApiClient.call_api)
    print(str(sig))
except Exception as e:
    print(f"Could not inspect signature: {e}")

print("\n--- DIAGNOSTIC: ApiClient Methods ---")
print([m for m in dir(ApiClient) if not m.startswith('_')])

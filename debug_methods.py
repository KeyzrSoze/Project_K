import inspect
from kalshi_python import ApiClient
from kalshi_python.api import markets_api

print("--- DIAGNOSTIC: MarketsApi Methods ---")
# We initialize it properly to see bound methods
client = ApiClient()
api = markets_api.MarketsApi(client)

# List all methods that don't start with underscore
methods = [m for m in dir(api) if not m.startswith('_')]
for m in methods:
    print(f" - {m}")

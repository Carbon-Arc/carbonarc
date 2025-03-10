import os
import pandas as pd
from pprint import pprint
from carbonarc import APIClient
from carbonarc.auth import TokenAuth

# Required environment variables
API_AUTH_TOKEN = os.environ.get("API_AUTH_TOKEN")
assert API_AUTH_TOKEN, "API_AUTH_TOKEN environment variable is not set."

# Initialize the API client with authentication
auth = TokenAuth(API_AUTH_TOKEN)
client = APIClient(auth_token=auth)

# Get insights data identifiers
data_identifiers = client.get_insights_data_idetifiers(page=1, page_size=10)
print("Data Identifiers:")
data_ids = data_identifiers["data"]
for data_id in data_ids:
    pprint(data_id)

# Get metadata for a specific data identifier
data_identifier = data_ids[0]["data_identifier"]
metadata = client.get_insight_metadata(data_identifier)
print(f"\nMetadata for {data_identifier}:")
pprint(metadata)

# Get filters for a specific data identifier
filters = client.get_insight_filters(data_identifier)
print(f"\nFilters for {data_identifier}:")
pprint(filters)

# Get filter values for a specific filter key
filter_keys = list(filters["filters"])
filter_key = filter_keys[0]["key"]
print(f"\nFilter key: {filter_key}")
filter_values = client.get_insight_filter_values(data_identifier, filter_key)
print(f"\nFilter values for {filter_key} in {data_identifier}:")
pprint(filter_values)


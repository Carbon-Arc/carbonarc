# Carbon Arc - Python Package

Client for [Carbon Arc](https://carbonarc.co/), an insight exchange platform.
It supports Python>=3.9.

## Usage

**Installation**

```
$ pip install git+https://github.com/Carbon-Arc/carbonarc.git
```

**Quick Start**

Initialize the API client with authentication.

```python
from carbonarc import APIClient

client = APIClient(
    token="<token>", # optional; set env var API_AUTH_TOKEN
)
```

## Examples

### Get insights data identifiers

```python
data_identifiers = client.get_insights_data_idetifiers(page=1, page_size=10)
print("Data Identifiers:")
data_ids = data_identifiers["data"]
for data_id in data_identifiers["data"]:
    pprint(data_id)
```

### Get metadata for a specific data identifier

```python
data_identifier = data_ids[0]["data_identifier"]
metadata = client.get_insight_metadata(data_identifier)
print(f"Metadata for {data_identifier}:")
pprint(metadata)
```

### Get filters for a specific data identifier

```python
filters = client.get_insight_filters(data_identifier)
print(f"Filters for {data_identifier}:")
pprint(filters)
```

### Get filter values for a specific filter key
```python
filter_keys = list(filters["filters"])
filter_key = filter_keys[0]["key"]
print(f"Filter key: {filter_key}")
filter_values = client.get_insight_filter_values(data_identifier, filter_key)
print(f"Filter values for {filter_key} in {data_identifier}:")
pprint(filter_values)
```
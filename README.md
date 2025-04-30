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

## Working with insights apis

### Get insights data identifiers

```python
data_identifiers = client.get_insights_data_identifiers(page=1, page_size=10)
print("Data Identifiers:")
data_ids = data_identifiers["data"]
for data_id in data_identifiers["data"]:
    print(data_id)
```

### Get metadata for a specific data identifier

```python
data_identifier = data_ids[0]["data_identifier"]
metadata = client.get_insight_metadata(data_identifier)
print(f"Metadata for {data_identifier}:")
print(metadata)
```

### Get filters for a specific data identifier

```python
filters = client.get_insight_filters(data_identifier)
print(f"Filters for {data_identifier}:")
print(filters)
```

### Get filter values for a specific filter key
```python
filter_keys = list(filters["filters"])
filter_key = filter_keys[0]["key"]
print(f"Filter key: {filter_key}")
filter_values = client.get_insight_filter_values(data_identifier, filter_key)
print(f"Filter values for {filter_key} in {data_identifier}:")
print(filter_values)
```

### Get insights data

```python
insights_data = client.get_insights_data(data_identifier, page=1, page_size=10)
print(f"Insights data for {data_identifier}:")
print(insights_data)
```

### Get insights data as pandas DataFrame

```python
insights_df = client.get_insights_data_pandas(data_identifier, page=1, page_size=10)
print(f"Insights data for {data_identifier} as DataFrame:")
print(insights_df)
```

## Working with alldata APIs

### Get alldata identifiersq

```python
data_identifiers = client.get_alldata_data_identifiers()
print("Data Identifiers:", data_identifiers)
data_ids_df = pd.DataFrame(data_identifiers["data"])
print(data_ids_df)
```

### Get manifest for a specific data identifier

```python
data_identifier = "card_us_detail_data"
manifest = client.get_alldata_manifest(data_identifier)
print(f"\nManifest for {data_identifier}:")
print(manifest)
```

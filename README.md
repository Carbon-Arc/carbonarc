# Carbon Arc - Python Package

Client for [Carbon Arc](https://carbonarc.co/), an Insights Exchange Platform.

## Usage

**Installation**

```bash
pip install carbonarc
```

**Quick Start**

Initialize the API client with authentication.

```python
from carbonarc import CarbonArcClient

client = CarbonArcClient(token="<token>") # retrieve token from account
```

## Prisms

Daily reference data: the same prisms and Arrays as the public Prisms page, read with your API token. Values are relative only (YoY index levels and shares).

```python
from carbonarc.prisms import array_to_dataframe, prism_to_dataframe

catalog = client.prisms.list_prisms()          # every live prism and Array, plus `tou`
prism = client.prisms.get_prism(catalog["prisms"][0]["prism_id"])  # one prism, with its `framework`
matches = client.prisms.list_prisms(insight_id=775)  # filter by insight_id and/or entity_id

arrays = client.prisms.list_arrays()["arrays"]
array = client.prisms.get_array(arrays[0]["array_id"])

prism_to_dataframe(prism)   # one row per entity, series and period
array_to_dataframe(array)   # one row per month
```

An unknown id raises a 404 error and an invalid token a 401.

## Resources

- [Tutorials](https://github.com/Carbon-Arc/carbonarc-tutorials)
- [Docs](https://docs.carbonarc.ai/)
- [App](https://app.carbonarc.ai/)
- [API](https://api.carbonarc.co/)
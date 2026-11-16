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

## Train (preview)

Train a forecasting model on frameworks you bought. **Preview:** it works only for accounts with Train turned on, and its methods can still change. For any other account, every call raises `TrainNotEnabled`.

Find what you can train on:

```python
from carbonarc.train import frameworks_to_dataframe

frameworks = client.train.list_frameworks()   # frameworks your account bought, at most 50
frameworks = client.train.list_frameworks(search="card spend")
frameworks_to_dataframe(frameworks)           # one row each: trainable, and the reason when not

framework_id = frameworks[0]["id"]
sample = client.train.get_framework_sample(framework_id)  # newest rows and each company's months

target = f"{framework_id}:card_spend"         # metric keys are in each framework's "metrics"
client.train.get_features([framework_id], target, window_start="2019-01", cutoff="2025-09")
client.train.get_suggestions([framework_id], target, window_start="2019-01", cutoff="2025-09")
```

Methods return the API's JSON with its keys as sent (camelCase). A refusal raises an exception from `carbonarc.utils.exceptions`:

| Exception | When |
| --- | --- |
| `TrainNotEnabled` | Train isn't turned on for this account. |
| `TrainNotFound` | No framework, run or model with that id for you (or Train isn't turned on). |
| `NoTrainingGrant` | The data's training rights don't allow it. |
| `NotEnoughData` | Too few series, a window too short, or a horizon too long. |
| `TrainRunExists` | You already have a run of this config; `existing_run_id` is its id. |
| `TrainBusy` | Every training machine is busy; `retry_after` says how long to wait. |
| `TrainPlatformError` | Something failed on Carbon Arc's side, such as the grant check being unreachable. Try again shortly. |

## Resources

- [Tutorials](https://github.com/Carbon-Arc/carbonarc-tutorials)
- [Docs](https://docs.carbonarc.ai/)
- [App](https://app.carbonarc.ai/)
- [API](https://api.carbonarc.co/)
"""client.train against a real power-api (dev).

Skipped unless these are set:
    CARBONARC_TOKEN   an API token for an account with Train turned on
    CARBONARC_HOST    the power-api base URL, e.g. the dev API host
Optional:
    CARBONARC_TOKEN_NO_TRAIN   a token for an account without Train

Run with:  pytest -m live
"""

import os

import pytest

from carbonarc import CarbonArcClient
from carbonarc.train import frameworks_to_dataframe
from carbonarc.utils.exceptions import TrainNotEnabled

TOKEN = os.environ.get("CARBONARC_TOKEN")
HOST = os.environ.get("CARBONARC_HOST")
NO_TRAIN_TOKEN = os.environ.get("CARBONARC_TOKEN_NO_TRAIN")

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not (TOKEN and HOST), reason="set CARBONARC_TOKEN and CARBONARC_HOST"
    ),
]


@pytest.fixture(scope="module")
def client():
    return CarbonArcClient(token=TOKEN, host=HOST)


def test_list_frameworks(client):
    frameworks = client.train.list_frameworks()
    df = frameworks_to_dataframe(frameworks)
    assert not df.empty
    # Every framework that can't be trained says why.
    assert df.loc[~df["trainable"], "reason"].notna().all()


def test_sample_of_a_trainable_framework(client):
    df = frameworks_to_dataframe(client.train.list_frameworks())
    trainable = df[df["trainable"]]
    if trainable.empty:
        pytest.skip("no trainable framework on this account")
    sample = client.train.get_framework_sample(trainable.iloc[0]["id"])
    assert sample["series"]


@pytest.mark.skipif(not NO_TRAIN_TOKEN, reason="set CARBONARC_TOKEN_NO_TRAIN")
def test_token_without_train_is_refused():
    with pytest.raises(TrainNotEnabled):
        CarbonArcClient(token=NO_TRAIN_TOKEN, host=HOST).train.list_frameworks()

"""client.train: finding what you can train on (PRO-6515), against recorded
responses shaped like power-api's Train schemas."""

import pytest

from carbonarc.train import TRAIN_NOT_ENABLED_MESSAGE, frameworks_to_dataframe
from carbonarc.utils.exceptions import (
    AuthenticationError,
    NotEnoughData,
    NotFoundError,
    NoTrainingGrant,
    TrainBusy,
    TrainNotEnabled,
    TrainNotFound,
    TrainPlatformError,
    TrainRunExists,
)
from tests.conftest import load_fixture, make_response

FW = "6f1c2b9e-0d5a-4a43-9a4e-1b2f3c4d5e01"


# -- list_frameworks ----------------------------------------------------------


def test_list_frameworks_returns_the_list(train_session):
    client, session = train_session
    session.queue(body=load_fixture("train/frameworks.json"))

    frameworks = client.train.list_frameworks()

    assert [f["id"] for f in frameworks][0] == FW
    assert len(frameworks) == 3
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("GET", "https://api.test/v2/train/frameworks")
    assert kwargs["params"] == {"trainable": "true"}


def test_list_frameworks_search_and_ids(train_session):
    client, session = train_session
    session.queue(body=load_fixture("train/frameworks.json"))
    session.queue(body=load_fixture("train/frameworks.json"))

    client.train.list_frameworks(search="card")
    client.train.list_frameworks(framework_ids=[FW])

    assert session.calls[0][2]["params"] == {"q": "card", "trainable": "true"}
    # By id lists what it asks for, trainable or not.
    assert session.calls[1][2]["params"] == {"ids": [FW]}
    with pytest.raises(ValueError):
        client.train.list_frameworks(search="card", framework_ids=[FW])


def test_list_frameworks_include_untrainable_sends_no_filter(train_session):
    client, session = train_session
    session.queue(body=load_fixture("train/frameworks.json"))

    client.train.list_frameworks(include_untrainable=True)

    assert session.calls[0][2]["params"] == {}


def test_list_frameworks_warns_what_it_left_out(train_session):
    client, session = train_session
    catalog = load_fixture("train/frameworks.json")
    catalog["leftOutTotal"] = 3
    catalog["leftOut"] = [
        {
            "reason": "This data has no training grant yet. You can still query and chart it.",
            "frameworks": [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}],
        },
        {
            "reason": "nothing in it Train can forecast.",
            "frameworks": [{"id": "c", "name": "C"}],
        },
    ]
    session.queue(body=catalog)

    with pytest.warns(UserWarning) as caught:
        client.train.list_frameworks()

    message = str(caught[0].message)
    assert message.startswith(
        "Left out 3 frameworks Train can't train on. "
        "Pass include_untrainable=True to list them."
    )
    assert "\n- 2: This data has no training grant yet." in message
    assert "\n- 1: nothing in it Train can forecast." in message


def test_list_frameworks_nothing_left_out_no_warning(train_session, recwarn):
    client, session = train_session
    catalog = load_fixture("train/frameworks.json")
    catalog["leftOutTotal"] = 0
    catalog["leftOut"] = []
    session.queue(body=catalog)

    client.train.list_frameworks()

    assert not [w for w in recwarn if "Left out" in str(w.message)]


def test_list_frameworks_warns_when_capped(train_session):
    client, session = train_session
    catalog = load_fixture("train/frameworks.json")
    catalog["total"] = 120
    session.queue(body=catalog)

    with pytest.warns(UserWarning, match="Listing 3 of 120"):
        client.train.list_frameworks()


def test_list_frameworks_without_train_raises_not_enabled(train_session):
    client, session = train_session
    session.queue(
        404,
        {"error_code": "FW-404", "detail": "Object not found: train catalog not found"},
    )

    with pytest.raises(TrainNotEnabled) as exc:
        client.train.list_frameworks()

    assert str(exc.value) == TRAIN_NOT_ENABLED_MESSAGE
    assert exc.value.status_code == 404
    assert exc.value.error_code == "FW-404"
    # Still a NotFoundError for code that catches the generic one.
    assert isinstance(exc.value, NotFoundError)


# -- frameworks_to_dataframe ----------------------------------------------------


def test_frameworks_to_dataframe_trainable_and_reason():
    frameworks = load_fixture("train/frameworks.json")["frameworks"]

    df = frameworks_to_dataframe(frameworks)

    assert list(df["trainable"]) == [True, False, False]
    assert df.loc[0, "reason"] is None
    # Not negotiated, in the New model page's words.
    assert df.loc[1, "reason"] == (
        "This data has no training grant yet. You can still query and chart it."
    )
    # Rights allow it, but Train can't build training data: the note.
    assert df.loc[2, "reason"] == "Train needs at least 10 series; this purchase has 3."
    assert df.loc[0, "metrics"] == ["card_spend"]
    assert df.loc[0, "window_start"] == "2019-01"


def test_frameworks_to_dataframe_rights_refused_names_supplier():
    framework = dict(
        load_fixture("train/frameworks.json")["frameworks"][0],
        trainability="no",
        trainable=False,
    )

    df = frameworks_to_dataframe([framework])

    assert df.loc[0, "reason"] == (
        "Fawn's terms don't allow training a model on it. "
        "You can still query and chart it."
    )


def test_frameworks_to_dataframe_blank_note_is_trainable():
    framework = dict(
        load_fixture("train/frameworks.json")["frameworks"][0], trainingNote=""
    )

    df = frameworks_to_dataframe([framework])

    assert bool(df.loc[0, "trainable"]) is True
    assert df.loc[0, "reason"] is None


def test_frameworks_to_dataframe_empty_keeps_columns(train_session):
    client, _ = train_session
    df = client.train.frameworks_to_dataframe([])
    assert df.empty
    assert "trainable" in df.columns and "reason" in df.columns


# -- sample, features, suggestions ---------------------------------------------


def test_get_framework_sample(train_session):
    client, session = train_session
    session.queue(body=load_fixture("train/sample.json"))

    sample = client.train.get_framework_sample(FW, series_by="company")

    assert sample["series"][0]["entityName"] == "Walmart"
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("GET", f"https://api.test/v2/train/frameworks/{FW}/sample")
    assert kwargs["params"] == {"seriesBy": "company"}


def test_get_framework_sample_unknown_id_raises_not_found(train_session):
    client, session = train_session
    session.queue(
        404,
        {"error_code": "FW-404", "detail": "Object not found: framework x not found"},
    )

    with pytest.raises(TrainNotFound, match="isn't enabled for this account"):
        client.train.get_framework_sample("x")


def test_not_found_message_has_one_period(train_session):
    client, session = train_session
    session.queue(
        404,
        {"error_code": "FW-404", "detail": "Object not found: Framework x not found."},
    )

    with pytest.raises(TrainNotFound) as exc:
        client.train.get_framework_sample("x")

    assert str(exc.value).startswith(
        "Object not found: Framework x not found. It doesn't exist"
    )


def test_500_without_a_code_is_a_platform_error(train_session):
    client, session = train_session
    session.queue(500, {"detail": None})

    with pytest.raises(TrainPlatformError, match=r"failed \(HTTP 500\)"):
        client.train.list_frameworks()


def test_get_features_sends_camelcase_body(train_session):
    client, session = train_session
    session.queue(body=load_fixture("train/features.json"))

    result = client.train.get_features(
        [FW],
        target=f"{FW}:card_spend",
        window_start="2019-01",
        cutoff="2025-09",
        horizon=3,
    )

    assert result["scored"] is True
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", "https://api.test/v2/train/features")
    assert kwargs["json"] == {
        "selectedIds": [FW],
        "target": f"{FW}:card_spend",
        "windowStart": "2019-01",
        "cutoff": "2025-09",
        "horizon": 3,
    }


def test_get_suggestions(train_session):
    client, session = train_session
    session.queue(body=load_fixture("train/suggestions.json"))

    result = client.train.get_suggestions(
        [FW], target=f"{FW}:card_spend", window_start="2019-01", cutoff="2025-09"
    )

    assert result["suggestions"][0]["name"] == "Foot traffic"
    method, url, kwargs = session.calls[0]
    assert (method, url) == ("POST", "https://api.test/v2/train/suggestions")
    assert "horizon" not in kwargs["json"]


def test_a_suggestion_buys_through_explorer_as_priced(train_session):
    """power-api returns each suggestion's priced framework in the shape
    /v2/framework/buy takes (PRO-6737), so the SDK's one buy path takes it
    as is."""
    client, session = train_session
    session.queue(body=load_fixture("train/suggestions.json"))
    suggestion = client.train.get_suggestions(
        [FW], target=f"{FW}:card_spend", window_start="2019-01", cutoff="2025-09"
    )["suggestions"][0]
    posted = []

    def fake_post(url, auth=None, data=None, json=None, **kwargs):
        posted.append((url, json))
        return make_response(body={"orders": []})

    client.explorer.request_manager.request_session.post = fake_post

    client.explorer.buy_frameworks(suggestion["framework"])

    url, body = posted[0]
    assert url.endswith("/framework/buy")
    framework = body["order"]["frameworks"][0]
    assert framework["insight"] == {"insight_id": 775}
    assert framework["entities"] == suggestion["framework"]["entities"]
    assert framework["filters"] == {"date_resolution": "month"}
    assert framework["aggregate"] == "sum"


# -- Refusal codes (the contract on PRO-5784) ----------------------------------


@pytest.mark.parametrize(
    "status, body, headers, expected",
    [
        (
            403,
            {
                "error_code": "TRAIN_RIGHTS",
                "detail": "Authorization error: No training grant.",
            },
            None,
            NoTrainingGrant,
        ),
        (
            400,
            {"error_code": "TRAIN_DATA", "detail": "Validation error: Too few series."},
            None,
            NotEnoughData,
        ),
        (
            500,
            {"error_code": "TRAIN_PLATFORM", "detail": "Our side."},
            None,
            TrainPlatformError,
        ),
        (
            # The rights service couldn't be reached: a 403, but not a refusal
            # of rights. The code wins over the status (contract, PRO-5784).
            403,
            {
                "error_code": "TRAIN_PLATFORM",
                "detail": "Authorization error: Couldn't check training rights.",
            },
            None,
            TrainPlatformError,
        ),
        (
            503,
            {"error_code": "TRAIN_BUSY", "detail": "Busy."},
            {"Retry-After": "120"},
            TrainBusy,
        ),
        (
            409,
            {
                "error_code": "TRAIN_DUPLICATE",
                "detail": {
                    "message": "Conflict error: Already running.",
                    "existingRunId": "run_1",
                },
            },
            None,
            TrainRunExists,
        ),
    ],
)
def test_refusal_codes_map_to_exceptions(
    train_session, status, body, headers, expected
):
    client, session = train_session
    session.queue(status, body, headers)

    with pytest.raises(expected) as exc:
        client.train.get_features(
            [FW], target="t", window_start="2019-01", cutoff="2025-09"
        )

    assert exc.value.status_code == status
    assert exc.value.error_code == body["error_code"]
    message = (
        body["detail"] if isinstance(body["detail"], str) else body["detail"]["message"]
    )
    assert str(exc.value) == message


def test_duplicate_and_busy_carry_their_extras(train_session):
    client, session = train_session
    session.queue(
        409,
        {
            "error_code": "TRAIN_DUPLICATE",
            "detail": "Conflict error: Already running.",
            "existingRunId": "run_1",
        },
    )
    session.queue(
        503, {"error_code": "TRAIN_BUSY", "detail": "Busy."}, {"Retry-After": "120"}
    )

    with pytest.raises(TrainRunExists) as dup:
        client.train.get_features(
            [FW], target="t", window_start="2019-01", cutoff="2025-09"
        )
    with pytest.raises(TrainBusy) as busy:
        client.train.get_features(
            [FW], target="t", window_start="2019-01", cutoff="2025-09"
        )

    assert dup.value.existing_run_id == "run_1"
    assert busy.value.retry_after == 120


def test_unnamed_errors_behave_as_elsewhere_in_the_sdk(train_session):
    client, session = train_session
    session.queue(401, {"detail": "Invalid token"})

    with pytest.raises(Exception) as exc:
        client.train.list_frameworks()

    # Not a Train refusal: the shared manager's requests.HTTPError, as for any
    # other client.
    assert not isinstance(exc.value, (TrainNotEnabled, AuthenticationError))
    assert exc.value.response.status_code == 401

import pandas as pd
import datetime
from typing import Union

def timeseries_response_to_pandas(response: Union[dict, pd.DataFrame]) -> pd.DataFrame:
    """
    Convert a timeseries response to a pandas DataFrame.
    :param response: The response object from the API.
    :return: A pandas DataFrame containing the timeseries data.
    """
    if isinstance(response, pd.DataFrame):
        response["date"] = pd.to_datetime(response["date"])
        return response
    elif isinstance(response, dict):
        current_page_data = response["data"]
        df = pd.DataFrame(current_page_data)
        df["date"] = pd.to_datetime(df["date"])
        return df
    else:
        raise ValueError("Response must be a dictionary or a pandas DataFrame")


def is_valid_date(date_string: str) -> bool:
    """
    Checks if a string is a valid date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format.
    :param date_string: The date string to check.
    :return: True if the date string is valid, False otherwise.
    """
    formats = ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']
    for fmt in formats:
        try:
            datetime.datetime.strptime(date_string, fmt)
            return True
        except ValueError:
            continue
    return False

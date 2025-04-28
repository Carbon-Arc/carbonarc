import pandas as pd
import datetime

def timeseries_response_to_pandas(response: dict) -> pd.DataFrame:
    """
    Convert a timeseries response to a pandas DataFrame.
    :param response: The response object from the API.
    :return: A pandas DataFrame containing the timeseries data.
    """
    dfs = []
    current_page_data = response["data"]
    for data in current_page_data:
        df = pd.DataFrame(data['data'])
        df["entity"] = data['entity']
        dfs.append(df)
    return pd.concat(dfs)


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

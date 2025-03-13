import pandas as pd

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

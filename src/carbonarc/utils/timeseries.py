import pandas as pd
import datetime
from typing import Union

def timeseries_response_to_pandas(response: Union[dict, pd.DataFrame]) -> pd.DataFrame:
    """
    Convert a timeseries response to a pandas DataFrame.

    Args:
        response: The response object from the API.

    Returns:
        A pandas DataFrame containing the timeseries data.
    """
    
    if isinstance(response, pd.DataFrame):
        response["date"] = pd.to_datetime(response["date"]).dt.date
        return response
    
    elif isinstance(response, dict):
        response_data = response.get("data", [])
        if not response_data:
            raise ValueError("Response data is empty")
        
        series = []
        for item in response_data:
            df = pd.DataFrame(item["series"])
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["entity_representation"] = item["entity_representation"]
            df["entity_id"] = item["entity_id"]
            df["entity_name"] = item["entity_name"]
            
            df.rename(columns={"value": item["insight"]}, inplace=True)
            
            df = df[["entity_representation", "entity_id", "entity_name", "date", item["insight"]]]
            series.append(df)
        
        df = pd.concat(series)
        
        return df
        
    else:
        raise ValueError("Response must be a dictionary or a pandas DataFrame")


def is_valid_date(date_string: str) -> bool:
    """
    Checks if a string is a valid date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format.
    
    Args:
        date_string: The date string to check.

    Returns:
        True if the date string is valid, False otherwise.
    """
    formats = ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']
    for fmt in formats:
        try:
            datetime.datetime.strptime(date_string, fmt)
            return True
        except ValueError:
            continue
    return False

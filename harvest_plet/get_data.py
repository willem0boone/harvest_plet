import datetime
import requests
from shapely import wkt as wkt_loader


def get_data(start_date: datetime.date,
             end_date: datetime.date,
             wkt: str,
             dataset_name: str):
    """
    Fetches data from the DASSH API based on date range, WKT polygon, and dataset name.

    :param start_date: Start date of data query.
    :type start_date: datetime.date
    :param end_date: End date of data query.
    :type end_date: datetime.date
    :param wkt: WKT polygon string.
    :type wkt: str
    :param dataset_name: Name of dataset to query.
    :type dataset_name: str
    :return: Response object or error message.
    :rtype: Any
    """

    # Check that end_date is after start_date
    if end_date <= start_date:
        raise ValueError("end_date must be after start_date")

    # Validate WKT string using shapely
    try:
        geom = wkt_loader.loads(wkt)
        if not geom.is_valid:
            raise ValueError("Provided WKT geometry is invalid")
    except Exception as e:
        raise ValueError(f"Invalid WKT format: {e}")

    # Format the API URL
    base_url = "https://www.dassh.ac.uk/plet/cgi-bin/get_form.py"
    params = {
        "startdate": start_date.strftime("%Y-%m-%d"),
        "enddate": end_date.strftime("%Y-%m-%d"),
        "wkt": wkt,
        "abundance_dataset": dataset_name,
        "format": "csv"
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise an error for HTTP errors
        return response.text  # Return CSV content or parse as needed
    except requests.RequestException as e:
        raise RuntimeError(f"Request failed: {e}")


if __name__ == "__main__":
    from datetime import date

    start = date(2010, 1, 1)
    end = date(2021, 1, 1)
    wkt_polygon = "POLYGON ((-180 -90,-180 90,180 90,180 -90,-180 -90))"
    dataset = "BE Flanders Marine Institute (VLIZ) - LW_VLIZ_zoo"

    csv_data = get_data(start, end, wkt_polygon, dataset)
    print(csv_data)




import io
import os
import re
import csv
import time
import datetime
import requests
from typing import Union
from datetime import date
from shapely import wkt as wkt_loader


def harvest_dataset(
    start_date: datetime.date,
    end_date: datetime.date,
    wkt: str,
    dataset_name: str,
    retries: int = 3,
    backoff_factor: float = 60.0,
    timeout: float = 600.0,
) -> str:
    """
    Fetch data from the DASSH API with retry and timeout logic.

    :param start_date: Start date of data query.
    :type start_date: datetime.date
    :param end_date: End date of data query.
    :type end_date: datetime.date
    :param wkt: WKT polygon string.
    :type wkt: str
    :param dataset_name: Name of dataset to query.
    :type dataset_name: str
    :param retries: Number of retries on failure. Defaults to 3.
    :type retries: int, optional
    :param backoff_factor: Seconds to wait between retries (exponential backoff). Defaults to 60.0.
    :type backoff_factor: float, optional
    :param timeout: Request timeout in seconds. Defaults to 600.0.
    :type timeout: float, optional
    :return: CSV data as string from the API response.
    :rtype: str
    :raises ValueError: If `end_date` is not after `start_date` or WKT is invalid.
    :raises RuntimeError: If all retry attempts fail.
    """
    if end_date <= start_date:
        raise ValueError("end_date must be after start_date")

    # Validate WKT string
    try:
        geom = wkt_loader.loads(wkt)
        if not geom.is_valid:
            raise ValueError("Provided WKT geometry is invalid")
    except Exception as e:
        raise ValueError(f"Invalid WKT format: {e}")

    base_url = "https://www.dassh.ac.uk/plet/cgi-bin/get_form.py"
    params = {
        "startdate": start_date.strftime("%Y-%m-%d"),
        "enddate": end_date.strftime("%Y-%m-%d"),
        "wkt": wkt,
        "abundance_dataset": dataset_name,
        "format": "csv",
    }

    attempt = 0
    while attempt < retries:
        try:
            response = requests.get(base_url, params=params, timeout=timeout)
            print(f"Request URL: {response.url}")
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            attempt += 1
            print(f"[Attempt {attempt}] Request failed: {e}")
            if attempt >= retries:
                raise RuntimeError(f"Request failed after {retries} attempts: {e}")
            sleep_time = backoff_factor * (2 ** (attempt - 1))
            print(f"Retrying in {sleep_time:.1f} seconds...")
            time.sleep(sleep_time)


def harvest_as_csv(

    start_date: datetime.date,
    end_date: datetime.date,
    wkt: str,
    dataset_name: str,
    out_dir: str,
    retries: int = 3,
    backoff_factor: float = 60.0,
    timeout: float = 600.0,


) -> None:
    """
    Harvest a dataset as CSV and save it to the specified directory.

    :param start_date: Start of the date range.
    :type start_date: datetime.date
    :param end_date: End of the date range.
    :type end_date: datetime.date
    :param wkt: WKT polygon string for the query.
    :type wkt: str
    :param dataset_name: Dataset to harvest.
    :type dataset_name: str
    :param out_dir: Directory to save the CSV file.
    :type out_dir: str
    :param retries: Number of retry attempts. Defaults to 3.
    :type retries: int, optional
    :param backoff_factor: Seconds to wait between retries (exponential backoff). Defaults to 60.0.
    :type backoff_factor: float, optional
    :param timeout: Timeout for the request in seconds. Defaults to 600.0.
    :type timeout: float, optional
    :return: None
    """
    # Ensure the output directory exists
    os.makedirs(out_dir, exist_ok=True)

    # Sanitize the file name
    clean_name = _sanitize_path_name(dataset_name)

    # Build output file path
    dest_path = os.path.join(out_dir, f"{clean_name}.csv")
    print(f"dest_path={dest_path}")

    try:
        # Harvest data
        csv_data = harvest_dataset(
            start_date,
            end_date,
            wkt,
            dataset_name,
            retries=retries,
            backoff_factor=backoff_factor,
            timeout=timeout,
        )

        # Write CSV data to file
        _write_csv_from_string(csv_data, dest_path)

    except Exception as e:
        print(f"Failed to harvest dataset '{dataset_name}': {e}")


def _sanitize_path_name(name: str) -> str:
    """
    Sanitize a string to be safe for use as a filename or directory name.

    Replaces spaces with underscores and removes all characters that are not
    alphanumeric, underscores, or hyphens. Converts the string to lowercase.

    :param name: The input string to sanitize.
    :type name: str
    :return: A sanitized string safe for file or directory names.
    :rtype: str
    """
    # Replace spaces with underscores
    name = name.replace(" ", "_")

    # Remove any characters that are not alphanumeric, underscore, or hyphen
    name = re.sub(r'[^\w\-]', '', name)

    # Convert to lowercase
    name = name.lower()

    return name


def _write_csv_from_string(data_str: str,
                           output_file: Union[str, bytes, os.PathLike]
                           ) -> None:
    """
    Writes CSV-formatted string data to a file.

    :param data_str: The CSV data as a string.
    :type data_str: str
    :param output_file: Path to the output CSV file.
    :type output_file: Union[str, bytes, os.PathLike]
    :return: None
    :rtype: None
    """
    csv_buffer = io.StringIO(data_str)
    reader = csv.reader(csv_buffer)

    with open(output_file, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        for row in reader:
            writer.writerow(row)

    print(f"CSV written to {output_file}")


if __name__ == "__main__":
    print('start')

    start = date(2010, 1, 1)
    end = date(2021, 1, 1)
    wkt_polygon = "POLYGON ((-180 -90,-180 90,180 90,180 -90,-180 -90))"
    dataset = "BE Flanders Marine Institute (VLIZ) - LW_VLIZ_zoo"

    # csv_data = harvest_dataset(start, end, wkt_polygon, dataset)
    # print(csv_data)
    my_dir = ("C:/Users/willem.boone/Documents/projects/dto-bioflow/"
              "DUC4.3/hervest_results_single0")

    harvest_as_csv(start, end, wkt_polygon, dataset, out_dir=my_dir)




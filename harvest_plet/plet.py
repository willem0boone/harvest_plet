import os
import re
import csv
import io
import time
import requests
from typing import List
from typing import Union
from typing import Dict
from datetime import date
from bs4 import BeautifulSoup
from shapely import wkt as wkt_loader


class PLETHarvester:
    """
    Interface to interact with the DASSH PLET database for harvesting marine
    biological datasets.

    Provides functionality to list datasets, query specific data using
    spatiotemporal filters (including OSPAR COMP areas), and save results to
    CSV.
    """

    BASE_URL: str = "https://www.dassh.ac.uk/plet/cgi-bin/get_form.py"
    SITE_URL: str = "https://www.dassh.ac.uk/lifeforms/"

    def __init__(self) -> None:
        """
        Initializes the PLETHarvester instance with a persistent requests
        session.
        """
        self.session = requests.Session()

    def get_dataset_names(self) -> List[str]:
        """
        Retrieve all available dataset names from the DASSH website.

        :returns: A list of dataset names available for download.
        :rtype: List[str]
        """
        response = self.session.get(self.SITE_URL)
        response.encoding = response.apparent_encoding  # Fix encoding issues
        soup = BeautifulSoup(response.text, 'html.parser')

        select_element = soup.find("select",
                                   {"id": "abundance_dataset"})
        if not select_element:
            print("Select element not found.")
            return []

        options = [
            option.text.strip()
            for option in select_element.find_all("option")
            if
            option.text.strip()
            and "select a dataset"
            not in option.text.lower()
        ]
        return options

    def harvest_dataset(
        self,
        start_date: date,
        end_date: date,
        wkt: str,
        dataset_name: str,
        retries: int = 3,
        backoff_factor: float = 60.0,
        timeout: float = 600.0
    ) -> str:
        """
        Download dataset from DASSH API for a given time range and spatial
        region.

        :param start_date: Start date of query.
        :type start_date: date
        :param end_date: End date of query.
        :type end_date: date
        :param wkt: WKT string representing the polygon region.
        :type wkt: str
        :param dataset_name: Dataset name to retrieve.
        :type dataset_name: str
        :param retries: Number of retry attempts on failure.
        :type retries: int
        :param backoff_factor: Backoff multiplier for retry wait time.
        :type backoff_factor: float
        :param timeout: Request timeout in seconds.
        :type timeout: float

        :returns: CSV content as a string.
        :rtype: str

        :raises ValueError: If inputs are invalid.
        :raises RuntimeError: If all retry attempts fail.
        """
        if end_date <= start_date:
            raise ValueError("end_date must be after start_date")

        try:
            geom = wkt_loader.loads(wkt)
            if not geom.is_valid:
                raise ValueError("Provided WKT geometry is invalid")
        except Exception as e:
            raise ValueError(f"Invalid WKT format: {e}")

        params = {
            "startdate": start_date.strftime("%Y-%m-%d"),
            "enddate": end_date.strftime("%Y-%m-%d"),
            "wkt": wkt,
            "abundance_dataset": dataset_name,
            "format": "csv",
        }

        for attempt in range(1, retries + 1):
            try:
                response = self.session.get(self.BASE_URL,
                                            params=params,
                                            timeout=timeout)
                # print(f"Request URL: {response.url}")
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                print(f"[Attempt {attempt}] Request failed: {e}")
                if attempt == retries:
                    raise RuntimeError(f"Request failed after {retries} "
                                       f"attempts: {e}")
                sleep_time = backoff_factor * (2 ** (attempt - 1))
                print(f"Retrying in {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)

    def harvest_as_csv(
        self,
        start_date: date,
        end_date: date,
        wkt: str,
        dataset_name: str,
        out_dir: str,
        name: str,
        retries: int = 3,
        backoff_factor: float = 60.0,
        timeout: float = 600.0
    ) -> None:
        """
        Harvest dataset and write output to a CSV file in the specified
        directory.

        :param start_date: Start of the time range.
        :type start_date: date
        :param end_date: End of the time range.
        :type end_date: date
        :param wkt: WKT string representing spatial bounds.
        :type wkt: str
        :param dataset_name: Dataset name to query.
        :type dataset_name: str
        :param out_dir: Output directory to save the file.
        :type out_dir: str
        :param name: name of csv file to create.
        :type name: str
        :param retries: Number of retry attempts.
        :type retries: int
        :param backoff_factor: Retry backoff factor in seconds.
        :type backoff_factor: float
        :param timeout: Timeout in seconds for the request.
        :type timeout: float

        :returns: None
        :rtype: None
        """
        os.makedirs(out_dir, exist_ok=True)
        clean_name = self._sanitize_path_name(name)
        dest_path = os.path.join(out_dir, f"{clean_name}.csv")

        try:
            csv_data = self.harvest_dataset(
                start_date=start_date,
                end_date=end_date,
                wkt=wkt,
                dataset_name=dataset_name,
                retries=retries,
                backoff_factor=backoff_factor,
                timeout=timeout
            )

            self._write_csv_from_string(csv_data, dest_path)
        except Exception as e:
            print(f"Failed to harvest dataset '{dataset_name}': {e}")

    def _harvest_all_datasets(
        self,
        start_date: date = date(2010, 1, 1),
        end_date: date = date(2021, 1, 1),
        wkt: str = "POLYGON ((-180 -90,-180 90,180 90,180 -90,-180 -90))",
        out_dir: str = "",
        retries: int = 3,
        backoff_factor: float = 60.0,
        timeout: float = 600.0
    ) -> Dict[str, List[str]]:
        """
        Harvest all available datasets over a given region and time period.

        :param start_date: Start of the date range.
        :type start_date: date
        :param end_date: End of the date range.
        :type end_date: date
        :param wkt: WKT string for the spatial region.
        :type wkt: str
        :param out_dir: Output directory to save harvested CSV files.
        :type out_dir: str
        :param retries: Retry attempts for each dataset.
        :type retries: int
        :param backoff_factor: Time multiplier for retries.
        :type backoff_factor: float
        :param timeout: Request timeout in seconds.
        :type timeout: float

        :returns: Dictionary with 'succeeded' and 'failed' dataset name lists.
        :rtype: Dict[str, List[str]]
        """
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir, exist_ok=True)
            print(f"Created output directory: {out_dir}")

        succeeded: List[str] = []
        failed: List[str] = []

        for dataset in self.get_dataset_names():
            print(f"Attempting to harvest dataset: {dataset}")
            try:
                self.harvest_as_csv(
                    dataset_name=dataset,
                    start_date=start_date,
                    end_date=end_date,
                    wkt=wkt,
                    out_dir=out_dir,
                    retries=retries,
                    backoff_factor=backoff_factor,
                    timeout=timeout,
                )
                succeeded.append(dataset)
            except Exception as e:
                failed.append(dataset)
                print(f"Failed to harvest dataset: {dataset}. Error: {e}")

        return {"succeeded": succeeded, "failed": failed}

    @staticmethod
    def _sanitize_path_name(name: str) -> str:
        """
        Clean string for safe use as file or directory name.

        :param name: String to sanitize.
        :type name: str

        :returns: Sanitized name in lowercase with no special characters.
        :rtype: str
        """
        name = name.replace(" ", "_")
        name = re.sub(r'[^\w\-]', '', name)
        return name

    @staticmethod
    def _write_csv_from_string(
        data_str: str,
        output_file: Union[str, bytes, os.PathLike],
    ) -> None:
        """
        Write CSV-formatted string data to disk.

        :param data_str: String containing CSV data.
        :type data_str: str
        :param output_file: Path to the output file.
        :type output_file: Union[str, bytes, os.PathLike]

        :returns: None
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
    plet_harvester = PLETHarvester()

    # test dateset names
    dataset_names = plet_harvester.get_dataset_names()
    print(dataset_names)

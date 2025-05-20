import os
from typing import List
from typing import Dict
from datetime import date
from harvest_plet.harvest_dataset import harvest_as_csv
from harvest_plet.list_datasets import get_dataset_names


def harvest_all_datasets(
    start_date: date = date(2010, 1, 1),
    end_date: date = date(2021, 1, 1),
    wkt: str = "POLYGON ((-180 -90,-180 90,180 90,180 -90,-180 -90))",
    out_dir: str = "",
    retries: int = 3,
    backoff_factor: float = 60.0,
    timeout: float = 600.0,
) -> Dict[str, List[str]]:
    """
    Harvest all datasets within the given time range and spatial area, saving results as CSV files.

    This function retrieves dataset names via `get_dataset_names()`, then attempts to
    harvest each dataset using `harvest_as_csv()`. All harvested datasets are saved
    as CSV files in `out_dir`. Successful and failed harvests are recorded without
    raising exceptions to the caller.

    :param start_date: Start of the date range. Defaults to 2010-01-01.
    :type start_date: datetime.date, optional
    :param end_date: End of the date range. Defaults to 2021-01-01.
    :type end_date: datetime.date, optional
    :param wkt: WKT polygon string for the query. Defaults to global polygon.
    :type wkt: str, optional
    :param out_dir: The directory where CSV output files will be saved.
    :type out_dir: str
    :param retries: The number of retry attempts for each dataset harvest upon failure.
    :type retries: int
    :param backoff_factor: The backoff factor in seconds between retries.
    :type backoff_factor: float
    :param timeout: The timeout in seconds for each harvest attempt.
    :type timeout: float
    :returns: A dictionary with two keys:
              `"succeeded"` maps to a list of dataset names that were harvested successfully,
              and `"failed"` maps to a list of dataset names that failed to harvest.
    :rtype: dict
    """
    # Ensure the output directory exists
    if not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)
        print(f"Created output directory: {out_dir}")

    succeeded: List[str] = []
    failed: List[str] = []

    # Iterate over all dataset names and attempt to harvest each one
    for dataset in get_dataset_names():
        print(f"Attempting to harvest dataset: {dataset}")
        try:
            # Attempt to harvest and save CSV
            harvest_as_csv(
                dataset_name=dataset,
                start_date=start_date,
                end_date=end_date,
                wkt=wkt,
                out_dir=out_dir,
                retries=retries,
                backoff_factor=backoff_factor,
                timeout=timeout
            )
            succeeded.append(dataset)
            print(f"Successfully harvested dataset: {dataset}")
        except Exception as e:
            # On failure, record and log the error
            failed.append(dataset)
            print(f"Failed to harvest dataset: {dataset}. Error: {e}")

    return {"succeeded": succeeded, "failed": failed}


if __name__ == "__main__":
    my_dir = ("C:/Users/willem.boone/Documents/projects/dto-bioflow/"
              "DUC4.3/hervest_results_all")
    results, failures = harvest_all_datasets(out_dir=my_dir)









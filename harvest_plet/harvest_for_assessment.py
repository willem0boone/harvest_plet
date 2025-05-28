import yaml
from datetime import date
from harvest_plet.ospar_comp import OSPARRegions
from harvest_plet.plet import PLETHarvester


def harvest_for_assessment(start_date: date,
                           end_date: date,
                           out_dir: str
                           ) -> None:
    """
    Harvests datasets for assessment based on region definitions and a YAML
    config.

    This function reads a configuration file `assessment.yml` that maps dataset
    names to region IDs. For each region, it retrieves the corresponding WKT
    geometry from the OSPARRegions object and uses the PLETHarvester to
    download and save the dataset as a CSV file. Filenames are dynamically
    generated based on dataset name, region ID, and date range.

    :param start_date: Start date of the data query.
    :type start_date: date
    :param end_date: End date of the data query.
    :type end_date: date
    :param out_dir: Directory where the output CSV files will be saved.
    :type out_dir: str

    :returns: None
    :rtype: None
    """
    # Initiate helper classes
    comp_regions = OSPARRegions()
    plet_harvester = PLETHarvester()

    # Load the assessment configuration file
    with open("assessment.yml", "r") as f:
        data: Dict[str, List[str]] = yaml.safe_load(f)

    # Iterate over datasets and associated region IDs
    for dataset_name, regions in data.items():
        for region_id in regions:
            print(f">>> Working on dataset: {dataset_name} "
                  f"- region: {region_id}")

            # Get the simplified WKT geometry for the region
            region_wkt = comp_regions.get_wkt(id=region_id, simplify=True)

            # Construct a unique output name
            name = (
                f"{dataset_name}"
                f"_{region_id}"
                f"_{start_date.strftime('%Y-%m-%d')}"
                f"_{end_date.strftime('%Y-%m-%d')}"
            )

            # Harvest and save the dataset
            plet_harvester.harvest_as_csv(
                start_date=start_date,
                end_date=end_date,
                wkt=region_wkt,
                dataset_name=dataset_name,
                out_dir=out_dir,
                name=name
            )


if __name__ == "__main__":
    start_date = date(2015, 1, 1)
    end_date = date(2025, 1, 1)
    out_dir = "C:/Users/willem.boone/Downloads/harvest"
    harvest_for_assessment(start_date=start_date,
                           end_date=end_date,
                           out_dir=out_dir)

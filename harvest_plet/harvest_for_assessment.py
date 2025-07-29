import os
import yaml
import s3fs
import warnings
import pandas as pd
from typing import Dict
from typing import List
from typing import Optional
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
            print(region_wkt)

            # Construct a unique output name
            name = (
                f"{dataset_name}"
                f"_{region_id}"
                f"_{start_date.strftime('%Y-%m-%d')}"
                f"_{end_date.strftime('%Y-%m-%d')}"
            )

            # Harvest and save the dataset
            plet_harvester.harvest_data(
                start_date=start_date,
                end_date=end_date,
                wkt=region_wkt,
                dataset_name=dataset_name,
                csv=True,
                out_dir=out_dir,
                name=name
            )


def to_parquet(
    csv_dir: str,
    out_path: Optional[str] = None,
    use_s3: bool = False,
    bucket: Optional[str] = None,
    endpoint_url: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None
) -> None:
    """
    Converts all CSV files in a directory to a single merged Parquet file.

    Can optionally save to S3/MinIO if `use_s3` is True and bucket credentials are provided.

    :param csv_dir: Directory containing CSV files.
    :type csv_dir: str
    :param out_path: Optional output path. If None:
                     - writes to 'merged.parquet' locally
                     - or 'merged.parquet' in the S3 bucket root if use_s3=True
    :param use_s3: Whether to write to S3 or MinIO.
    :type use_s3: bool
    :param bucket: Name of the S3/MinIO bucket.
    :type bucket: Optional[str]
    :param endpoint_url: S3/MinIO endpoint URL.
    :type endpoint_url: Optional[str]
    :param aws_access_key_id: S3/MinIO access key ID.
    :type aws_access_key_id: Optional[str]
    :param aws_secret_access_key: S3/MinIO secret access key.
    :type aws_secret_access_key: Optional[str]
    :param aws_session_token: Optional session token.
    :type aws_session_token: Optional[str]

    :returns: None
    :rtype: None
    """
    if not os.path.isdir(csv_dir):
        raise ValueError(f"Directory does not exist: {csv_dir}")

    csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]
    if not csv_files:
        raise ValueError(f"No CSV files found in: {csv_dir}")

    dataframes = []
    for file in csv_files:
        file_path = os.path.join(csv_dir, file)
        try:
            df = pd.read_csv(file_path)
            df["__source_file__"] = file  # Optional: trace origin
            dataframes.append(df)
        except Exception as e:
            warnings.warn(f"Skipping file {file} due to error: {e}")

    if not dataframes:
        raise RuntimeError("No valid CSV files could be read.")

    merged_df = pd.concat(dataframes, ignore_index=True)

    # Define output path
    if out_path is None:
        out_path = "merged.parquet"

    if use_s3:
        if not all([bucket, endpoint_url, aws_access_key_id, aws_secret_access_key]):
            raise ValueError("Missing S3 credentials or bucket configuration.")

        fs = s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            token=aws_session_token,
            client_kwargs={"endpoint_url": endpoint_url}
        )

        s3_path = f"{bucket}/{out_path}".lstrip("/")
        with fs.open(s3_path, "wb") as f:
            merged_df.to_parquet(f, index=False, engine="pyarrow")
        print(f"Parquet file saved to S3 at: s3://{s3_path}")
    else:
        merged_df.to_parquet(out_path, index=False)
        print(f"Parquet file saved locally at: {out_path}")


if __name__ == "__main__":
    from _config import Config

    config = Config()
    print(config.settings)

    start_date = date(2015, 1, 1)
    end_date = date(2025, 1, 1)
    out_dir = "C:/Users/willem.boone/Downloads/harvest_"
    harvest_for_assessment(start_date=start_date,
                           end_date=end_date,
                           out_dir=out_dir)
    to_parquet(
        csv_dir=out_dir,
        out_path="merged-data.parquet",
        use_s3=True,
        bucket=config.settings.bucket,
        endpoint_url=config.settings.endpoint_url,
        aws_access_key_id=config.settings.aws_access_key_id,
        aws_secret_access_key=config.settings.aws_secret_access_key,
        aws_session_token=config.settings.aws_session_token
    )

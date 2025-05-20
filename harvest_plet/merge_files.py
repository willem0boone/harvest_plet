import os
import pandas as pd


def merge_csvs_with_dataset_name(input_dir: str, output_file: str):
    """
    Merges all CSV files in a directory, adding a 'dataset_name' column.

    :param input_dir: Directory containing CSV files.
    :param output_file: Path to save the merged CSV file.
    """
    all_dfs = []

    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_dir, filename)
            try:
                df = pd.read_csv(file_path)
                dataset_name = os.path.splitext(filename)[0]  # Remove .csv
                df['dataset_name'] = dataset_name
                all_dfs.append(df)
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    if not all_dfs:
        print("No CSVs found or all failed to load.")
        return

    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df.to_csv(output_file, index=False)
    print(f"Merged {len(all_dfs)} files to {output_file}")
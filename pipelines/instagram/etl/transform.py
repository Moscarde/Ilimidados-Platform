import os
import re

import pandas as pd
from etl.transform_tables import apply_transformations


def parse_to_dataframe(content):
    """
    Converts the given content into a pandas DataFrame.

    Parameters:
        content (list): The content to convert into a DataFrame.

    Returns:
        pd.DataFrame: The resulting DataFrame.
    """
    from io import StringIO

    csv_content = StringIO("".join(content))
    return pd.read_csv(csv_content)


def extract_table_from_file(filepath):
    """
    Extracts data from the given file by splitting its content into sections and returning them as DataFrames.

    Parameters:
        filepath (str): The path to the file to be processed.

    Returns:
        dict: A dictionary with headers as keys and DataFrames as values.
    """
    dataframes = {}

    try:
        with open(filepath, "r", encoding="utf-16") as file:
            lines = file.readlines()

        if lines[0].startswith("sep=,"):
            lines = lines[1:]

            content_parts = []
            current_part = []

            for line in lines:
                if line.strip() == "":
                    if current_part:
                        content_parts.append(current_part)
                    current_part = []
                else:
                    current_part.append(line)

            if current_part:
                content_parts.append(current_part)

            for part in content_parts:
                header_name = part[0].strip().replace('"', "").replace(" ", "_")
                part_content = part[1:]
                dataframes[header_name] = parse_to_dataframe(part_content)
        else:
            header_name = os.path.basename(filepath).split(".")[0]
            dataframes[header_name] = parse_to_dataframe(lines)

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

    return dataframes


def read_table_from_file(filepath):
    """
    Reads data from a file and returns it as a DataFrame.

    Parameters:
        filepath (str): The path to the file to be read.

    Returns:
        dict: A dictionary with table names as keys and DataFrames as values.
    """
    try:
        df = pd.read_csv(filepath)
        if "Toques em figurinhas" in df.columns:
            return {"instagram_content_story_data_raw": df}
        else:
            return {"instagram_content_post_data_raw": df}
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return None


def read_raw_data(directory):
    """
    Iterates over files in a directory and processes them based on their filename pattern.

    Parameters:
        directory (str): The path to the directory containing files.

    Returns:
        dict: A dictionary with table names as keys and DataFrames as values.
    """
    data = {}

    pattern = re.compile(r".*\d{13,}.*\.csv$")

    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)

        if os.path.isfile(filepath):
            if pattern.match(filename):
                table_data = read_table_from_file(filepath)
                if table_data:
                    data.update(table_data)
            elif filename.endswith(".csv"):
                table_data = extract_table_from_file(filepath)
                if table_data:
                    data.update(table_data)

    return data


def transform_data(extraction_dir):
    """
    Processes raw data from files in the specified directory and applies transformations to it.

    Parameters:
        extraction_dir (str): The directory containing raw data files.

    Returns:
        dict: A dictionary with transformed data after applying the transformations.
    """

    raw_data = read_raw_data(extraction_dir)

    transformed_data = apply_transformations(raw_data)

    return transformed_data

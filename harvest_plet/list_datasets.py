import requests
from bs4 import BeautifulSoup
import urllib.parse


def get_dataset_names() -> list:
    """
    Retrieves a list of dataset names from the DASSH website.

    Parses the HTML content of the target webpage and extracts dataset names
    from the <select> element with ID 'abundance_dataset'.

    :return: List of dataset names.
    :rtype: list
    """
    response = requests.get("https://www.dassh.ac.uk/lifeforms/")
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    select_element = soup.find(
        "select", {"id": "abundance_dataset"})

    if select_element:
        options = [
            option.text.strip() for option in select_element.find_all("option")
            if option.get("value") != ""]
        return options
    else:
        print("Select element not found.")


def encode_dataset_name(name: str) -> str:
    """
    Encodes a dataset name to be safely used in a URL.

    :param name: The dataset name to encode.
    :type name: str
    :return: URL-encoded version of the dataset name.
    :rtype: str
    """
    return urllib.parse.quote(name)


if __name__ == "__main__":
    datasets = get_dataset_names()
    print(datasets)



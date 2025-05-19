import unittest
from unittest.mock import patch, Mock
from harvest_plet.list_datasets import get_dataset_names
from harvest_plet.list_datasets import encode_dataset_name


class TestDatasetFunctions(unittest.TestCase):
    @patch('harvest_plet.list_datasets.requests.get')
    def test_get_dataset_names_success(self, mock_get):
        mock_html = """
        <html>
            <body>
                <select id="abundance_dataset">
                    <option value="">-- Select --</option>
                    <option value="1">Dataset One</option>
                    <option value="2">Dataset Two</option>
                </select>
            </body>
        </html>
        """
        mock_response = Mock()
        mock_response.text = mock_html
        mock_get.return_value = mock_response

        expected = ["Dataset One", "Dataset Two"]
        result = get_dataset_names()
        self.assertEqual(result, expected)

    @patch('harvest_plet.list_datasets.requests.get')
    def test_get_dataset_names_no_select(self, mock_get):
        mock_html = "<html><body><p>No select here</p></body></html>"
        mock_response = Mock()
        mock_response.text = mock_html
        mock_get.return_value = mock_response

        result = get_dataset_names()
        self.assertIsNone(result)

    def test_encode_dataset_name(self):
        name = "BE Flanders Marine Institute (VLIZ) - LW_VLIZ_zoo"
        expected = ("BE%20Flanders%20Marine%20Institute%20%28VLIZ%29%20-%20LW_"
                    "VLIZ_zoo")
        result = encode_dataset_name(name)
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()

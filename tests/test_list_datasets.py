import unittest
from unittest.mock import patch, Mock
from harvest_plet.list_datasets import get_dataset_names


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


if __name__ == "__main__":
    unittest.main()

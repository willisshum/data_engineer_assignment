import unittest

from pipeline import ingest_csv

class TestPipeLine(unittest.TestCase):
    def test_ingest_csv(self):
        """Test that it can ingest csv correctly.
        """
        csv_path = "sample_data/sample-legacy-data.csv"
        csv_data_separator = ","
        df_testing = ingest_csv(csv_path, csv_data_separator)
        self.assertEqual(df_testing.shape, (100, 13), "100 records with 13 columns should be read")

if __name__ == "__main__":
    unittest.main()
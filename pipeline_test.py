import unittest
import pandas as pd
import re

from pipeline import ingest_csv, cleanse_data

class TestPipeLine(unittest.TestCase):
    def test_ingest_csv(self):
        """Test that it can ingest csv correctly.
        """
        csv_path = "sample_data/sample-legacy-data.csv"
        csv_data_separator = ","
        df_testing = ingest_csv(csv_path, csv_data_separator)
        self.assertEqual(df_testing.shape, (100, 13), "100 records with 13 columns should be read")

    def test_cleanse_data(self):
        """Test that some columns with "reject" in names are inserted.
        """
        data_testing = {
			"EntityID": [
                "1009"
            ],
            "EntityName": [
                "SunTech Industries"
            ],
            "EntityType": [
                "Company"
            ],
            "RegistrationNumber": [
                "REG55789"
            ],
            "IncorporationDate": [
                "3/10/15"
            ],
            "Country": [
                "Australia"
            ],
            "CountryCode": [
                "AU"
            ],
            "State": [
                "New South Wales"
            ],
            "StateCode": [
                "NSW"
            ],
            "Status": [
                "Active"
            ],
            "Industry": [
                "Technology"
            ],
            "ContactEmail": [
                "contact@suntech.com.au"
            ],
            "LastUpdate": [
                "4/24/22"
            ]
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_result = cleanse_data(df_testing)
        column_difference = set(df_result.columns) - set(df_testing.columns)
        self.assertGreater(len(column_difference), 0, "New columns should be inserted.")
        check_column_name_reject = [True if re.fullmatch(r".*reject$", x) is not None else False for x in column_difference]
        self.assertEqual(all(check_column_name_reject), True, 'All new columns should contain "reject" wordings.')

if __name__ == "__main__":
    unittest.main()
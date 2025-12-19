import unittest
import pandas as pd
import re
from pandas.testing import assert_frame_equal

from pipeline import ingest_csv, cleanse_data, process_entityName, process_entityType, process_registrationNumber, process_incorporationDate

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

    def test_process_entityName(self):
        """Test that it can process EntityName.
        """
        data_testing = {
            "EntityName": [
                "ABC",
                "",
                " ",
                None
            ]
        }
        data_expected = {
            "EntityName": [
                "ABC",
                "",
                "",
                None
            ],
            "EntityName_reject": [
                False,
                True,
                True,
                True
            ]
        }
        dtype_mapping = {
            "EntityName": "string",
            "EntityName_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_entityName(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_process_entityType(self):
        """Test that it can process EntityType.
        """
        data_testing = {
            "EntityType": [
                "Company",
                "Nonprofit",
                "Partnership",
                "Trust",
                "nonprofit",
                "pARTNERSHIP",
                "Trust2",
                "tRUST3",
                None
            ]
        }
        data_expected = {
            "EntityType": [
                "Company",
                "Nonprofit",
                "Partnership",
                "Trust",
                "Nonprofit",
                "Partnership",
                "Trust2",
                "Trust3",
                None
            ],
            'EntityType_reject': [
                False,
                False,
                False,
                False,
                False,
                False,
                True,
                True,
                True
            ]
        }
        dtype_mapping = {
            "EntityType": "string",
            "EntityType_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_entityType(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_process_registrationNumber(self):
        """Test that it can process RegistrationNumber.
        """
        data_testing = {
            "RegistrationNumber": [
                "REG10234",
                "reg10234",
                "REG000",
                "abc10234",
                "abc",
                None
            ]
        }
        data_expected = {
            "RegistrationNumber": [
                "REG10234",
                "REG10234",
                "REG000",
                "ABC10234",
                "ABC",
                None
            ],
            "RegistrationNumber_reject": [
                False,
                False,
                True,
                True,
                True,
                False
            ]
        }
        dtype_mapping = {
            "RegistrationNumber": "string",
            "RegistrationNumber_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_registrationNumber(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_process_incorporationDate(self):
        """Test that it can process IncorporationDate.
        """
        data_testing = {
            "IncorporationDate": [
                "9/17/21",
                "18/6/18",
                "9/17/2021",
                "18/6/2018",
                "11-26-17",
                "11-26-2017",
                "2017-11-26",
                "2-Nov-20",
                "asdf",
                None
            ]
        }
        data_expected = {
            "IncorporationDate": [
                "2021-09-17",
                "2018-06-18",
                "2021-09-17",
                "2018-06-18",
                "2017-11-26",
                "2017-11-26",
                "2017-11-26",
                "2020-11-02",
                "asdf",
                None
            ],
            "IncorporationDate_reject": [
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                True,
                False
            ]
        }
        dtype_mapping = {
            "IncorporationDate": "string",
            "IncorporationDate_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_incorporationDate(df_testing)
        assert_frame_equal(df_testing, df_expected)

if __name__ == "__main__":
    unittest.main()

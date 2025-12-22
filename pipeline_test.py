import unittest
import pandas as pd
import re
from pandas.testing import assert_frame_equal

from pipeline import ingest_csv, cleanse_data, process_entityName, process_entityType, process_registrationNumber, process_incorporationDate, process_countryCode, process_stateCode, process_status, process_industry, process_contactEmail, process_lastUpdate, deduplicate_records, validate_business_rules

class TestPipeLine(unittest.TestCase):
    def test_ingest_csv(self):
        """Test that it can ingest csv correctly.
        """
        csv_path = "sample_data/sample-legacy-data.csv"
        csv_data_separator = ","
        df_testing = ingest_csv(csv_path, csv_data_separator)
        self.assertEqual(df_testing.shape, (100, 13), "100 records with 13 columns should be read")

    def test_cleanse_data(self):
        """Test that some columns with "reject" or "revised" in names are inserted.
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
        check_column_name_reject = [True if re.fullmatch(r".*(reject|revised)$", x) is not None else False for x in column_difference]
        self.assertEqual(all(check_column_name_reject), True, 'All new columns should contain "reject" or "revised" wordings.')

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

    def test_process_countryCode(self):
        """Test that it can process CountryCode.
        """
        data_testing = {
            "CountryCode": [
                "CA",
                "us",
                "MY-15",
                "GB-EAW",
                "asdf",
                "asdf",
                "asdf",
                None,
                None,
                None
            ],
            "Country": [
                "Canada",
                "US",
                "Malaysia",
                "United Kingdom",
                "Singapore",
                "asdf2",
                None,
                "Germany",
                "asdf",
                None
            ]
        }
        data_expected = {
            "CountryCode_revised": [
                "CA",
                "US",
                "MY",
                "GB",
                "SG",
                "asdf2",
                None,
                "DE",
                "asdf",
                None
            ],
            "CountryCode_reject": [
                False,
                False,
                False,
                False,
                False,
                True,
                True,
                False,
                True,
                True
            ]
        }
        dtype_mapping = {
            "CountryCode_revised": "string",
            "CountryCode_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_countryCode(df_testing)
        assert_frame_equal(df_testing[["CountryCode_revised", "CountryCode_reject"]], df_expected)

    def test_process_stateCode(self):
        """Test that it can process StateCode.
        """
        data_testing = {
            "StateCode": [
                "VIC",
                None,
                None,
                "AU",
                "SCOT",
                "SCT",
                None,
                None,
                "asdf"
        ],
            "State": [
                "Victoria",
                None,
                "Victoria",
                None,
                "Scotland",
                "Scotland",
                None,
                "Labuan",
                "Bavaria"
            ],
            "CountryCode": [
                "AU",
                "AU",
                "AU",
                "AU",
                "GB",
                "GB",
                "GB-EAW",
                "MY-15",
                "DE"
            ],
            "CountryCode_revised": [
                "AU",
                "AU",
                "AU",
                "AU",
                "GB",
                "GB",
                "GB",
                "MY",
                "DE"
            ]
        }
        data_expected = {
            "StateCode_revised": [
                "VIC",
                None,
                "VIC",
                None,
                "SCT",
                "SCT",
                None,
                "15",
                "BY"
            ],
            "StateCode_reject": [
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                False
            ]
        }
        dtype_mapping = {
            "StateCode_revised": "string",
            "StateCode_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_stateCode(df_testing)
        assert_frame_equal(df_testing[["StateCode_revised", "StateCode_reject"]], df_expected)

    def test_process_status(self):
        """Test that it can process Status.
        """
        data_testing = {
            "Status": [
                "Active",
                "Inactive",
                "Pending",
                "N",
                "Actived",
                "Y",
                "asdf",
                None
            ]
        }
        data_expected = {
            "Status": [
                "Active",
                "Inactive",
                "Pending",
                "Inactive",
                "Active",
                "Active",
                "asdf",
                None
            ],
            "Status_reject": [
                False,
                False,
                False,
                False,
                False,
                False,
                True,
                True
            ]
        }
        dtype_mapping = {
            "Status": "string",
            "Status_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_status(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_process_industry(self):
        """Test that it can process Industry.
        """
        data_testing = {
            "Industry": [
                "Real Estate",
                "rEAL eSTATE",
                "Trust",
                "NULL",
                None
            ]
        }
        data_expected = {
            "Industry": [
                "Real Estate",
                "Real Estate",
                "Trust",
                None,
                None
            ],
            "Industry_reject": [
                False,
                False,
                False,
                False,
                False
            ]
        }
        dtype_mapping = {
            "Industry": "string",
            "Industry_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_industry(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_process_contactEmail(self):
        """Test that it can process ContactEmail.
        """
        data_testing = {
            "ContactEmail": [
                "info@freshfarm.in",
                "goldenGate.me",
                None
            ]
        }
        data_expected = {
            "ContactEmail": [
                "info@freshfarm.in",
                "goldenGate.me",
                None
            ],
            "ContactEmail_reject": [
                False,
                True,
                False
            ]
        }
        dtype_mapping = {
            "ContactEmail": "string",
            "ContactEmail_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_contactEmail(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_process_lastUpdate(self):
        """Test that it can process LastUpdate.
        """
        data_testing = {
            "LastUpdate": [
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
            "LastUpdate": [
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
            "LastUpdate_reject": [
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
            "LastUpdate": "string",
            "LastUpdate_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = process_lastUpdate(df_testing)
        assert_frame_equal(df_testing, df_expected)

    def test_deduplicate_records(self):
        """Test that it can deduplicate records.
        """
        data_testing = {
            "EntityID": [
                "1001",
                "1004",
                "1008",
                "1029",
                "1030",
                "1096",
                "2000"
            ],
            "EntityName": [
                "Acme Manufacturing",
                "Acme Manufacturing",
                "Vivo Trading",
                "Acme Manufacturing",
                "Vivo Trading",
                "Bluebell Trust",
                "Bluebell Trust"
            ],
            "EntityType": [
                "Company",
                "Company",
                "Company",
                "Company",
                "Company",
                "Trust",
                "Trust"
            ],
            "RegistrationNumber": [
                "REG10234",
                "REG10234",
                None,
                "REG10234",
                None,
                "REG33817",
                "REG33817"
            ],
            "IncorporationDate": [
                "5/12/10",
                "12/5/10",
                "4/17/20",
                "5/12/10",
                "4/17/20",
                "10/8/10",
                "10/8/10"
            ],
            "CountryCode_revised": [
                "US",
                "US",
                "US",
                "US",
                "US",
                "AU",
                "AU"
            ],
            "StateCode_revised": [
                "CA",
                "CA",
                "TX",
                None,
                None,
                "SYD",
                "SYD"
            ],
            "Status": [
                "Active",
                "Active",
                "Active",
                "Active",
                "Active",
                "Active",
                "Active"
            ],
            "Industry": [
                "Manufacturing",
                "Manufacturing",
                "Trading",
                "Manufacturing",
                "Trading",
                "Trust",
                "Trust"
            ],
            "ContactEmail": [
                "info@acmemfg.com",
                "info@acmemfg.com",
                None,
                "info@acmemfg.com",
                None,
                "info@bluebelltrust.au",
                "info@bluebelltrust.au"
            ],
            "LastUpdate": [
                "6/15/22",
                None,
                "3/9/22",
                "6/15/22",
                "3/9/22",
                "5/30/22",
                "5/30/22"
            ]
        }
        data_expected_deduplicate = {
            "EntityID": [
                "1096"
            ],
            "EntityName": [
                "Bluebell Trust"
            ],
            "EntityType": [
                "Trust"
            ],
            "RegistrationNumber": [
                "REG33817"
            ],
            "IncorporationDate": [
                "10/8/10"
            ],
            "CountryCode_revised": [
                "AU"
            ],
            "StateCode_revised": [
                "SYD"
            ],
            "Status": [
                "Active"
            ],
            "Industry": [
                "Trust"
            ],
            "ContactEmail": [
                "info@bluebelltrust.au"
            ],
            "LastUpdate": [
                "5/30/22"
            ],
            "duplicate_candidate": [
                True
            ]
        }
        data_expected_duplicate_reject = {
            "EntityID": [
                "1001",
                "1004",
                "1008",
                "1029",
                "1030"
            ],
            "EntityName": [
                "Acme Manufacturing",
                "Acme Manufacturing",
                "Vivo Trading",
                "Acme Manufacturing",
                "Vivo Trading"
            ],
            "EntityType": [
                "Company",
                "Company",
                "Company",
                "Company",
                "Company"
            ],
            "RegistrationNumber": [
                "REG10234",
                "REG10234",
                None,
                "REG10234",
                None
            ],
            "IncorporationDate": [
                "5/12/10",
                "12/5/10",
                "4/17/20",
                "5/12/10",
                "4/17/20"
            ],
            "CountryCode_revised": [
                "US",
                "US",
                "US",
                "US",
                "US"
            ],
            "StateCode_revised": [
                "CA",
                "CA",
                "TX",
                None,
                None
            ],
            "Status": [
                "Active",
                "Active",
                "Active",
                "Active",
                "Active"
            ],
            "Industry": [
                "Manufacturing",
                "Manufacturing",
                "Trading",
                "Manufacturing",
                "Trading"
            ],
            "ContactEmail": [
                "info@acmemfg.com",
                "info@acmemfg.com",
                None,
                "info@acmemfg.com",
                None
            ],
            "LastUpdate": [
                "6/15/22",
                None,
                "3/9/22",
                "6/15/22",
                "3/9/22"
            ],
            "duplicate_candidate": [
                True,
                True,
                True,
                True,
                True
            ]
        }
        dtype_mapping = {
            "EntityID": "string",
            "EntityName": "string",
            "EntityType": "string",
            "RegistrationNumber": "string",
            "IncorporationDate": "string",
            "CountryCode_revised": "string",
            "StateCode_revised": "string",
            "Status": "string",
            "Industry": "string",
            "ContactEmail": "string",
            "LastUpdate": "string",
            "duplicate_candidate": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected_deduplicate = pd.DataFrame(data_expected_deduplicate).astype(dtype_mapping)
        df_expected_duplicate_reject = pd.DataFrame(data_expected_duplicate_reject).astype(dtype_mapping)
        df_testing_deduplicate, df_testing_duplicate_reject = deduplicate_records(df_testing)
        assert_frame_equal(df_testing_deduplicate.sort_values(by=["EntityID"], ignore_index=True), df_expected_deduplicate.sort_values(by=["EntityID"], ignore_index=True))
        assert_frame_equal(df_testing_duplicate_reject.sort_values(by=["EntityID"], ignore_index=True), df_expected_duplicate_reject.sort_values(by=["EntityID"], ignore_index=True))

    def test_validate_business_rules(self):
        """Test that it can process LastUpdate.
        """
        data_testing = {
            "IncorporationDate": [
                "2017-11-26",
                None
            ]
        }
        data_expected = {
            "IncorporationDate": [
                "2017-11-26",
                None
            ],
            "business_rules_reject": [
                False,
                True
            ]
        }
        dtype_mapping = {
            "IncorporationDate": "string",
            "business_rules_reject": "bool"
        }
        df_testing = pd.DataFrame(data_testing, dtype=pd.StringDtype())
        df_expected = pd.DataFrame(data_expected).astype(dtype_mapping)
        df_testing = validate_business_rules(df_testing)
        assert_frame_equal(df_testing, df_expected)

if __name__ == "__main__":
    unittest.main()

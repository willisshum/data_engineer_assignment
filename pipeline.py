from dotenv import load_dotenv
import os
import logging
import pandas as pd
import re
from datetime import date
import pycountry
from translate import Translator
import mysql.connector
from mysql.connector import errorcode

from reference_value import LIST_ENTITY_TYPE, REGEX_PATTERN_REGISTRATION_NUMBER, REGEX_PATTERN_DATE_FORMAT, DATE_FORMAT_CODE_OUTPUT, REGEX_PATTERN_COUNTRY_CODE_OUTPUT, LIST_STATUS, DICT_STATUS_MAPPING, LIST_SCHEMA_MAPPING, QUERY_CREATE_TABLE_ENTITIES, QUERY_INSERT_UPDATE_ENTITY

load_dotenv()
DICT_LOG_LEVEL_REFERENCE = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG
}
LOG_LEVEL = DICT_LOG_LEVEL_REFERENCE[os.environ.get("LOG_LEVEL", "INFO")]
CSV_PATH = os.environ.get("CSV_PATH")
if not CSV_PATH:
    raise Exception("CSV path in .env is needed to continue!")
CSV_DATA_SEPARATOR = os.environ.get("CSV_DATA_SEPARATOR", ",")
MYSQL_CONNECTION_CREDENTIAL = {
    "HOST": os.environ.get("MYSQL_HOST"),
    "PORT": os.environ.get("MYSQL_PORT"),
    "USER": os.environ.get("MYSQL_USER"),
    "PASSWORD": os.environ.get("MYSQL_PASSWORD"),
    "SCHEMA": os.environ.get("MYSQL_SCHEMA"),
    "TABLE_ENTITIES": os.environ.get("MYSQL_TABLE_ENTITIES")
}
if not all(MYSQL_CONNECTION_CREDENTIAL.values()):
    raise Exception("MySQL connection credentials in .env are needed to continue!")

logging.basicConfig(
    format="[%(asctime)s][%(name)-5s][%(levelname)-5s] %(message)s (%(filename)s:%(lineno)d)",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=LOG_LEVEL
)

def ingest_csv(file_path, separator):
    """Ingest CSV data.

    Args:
        file_path (str): The path of the CSV file.
        separator (str): The separator in the CSV file.

    Returns:
        df (dataframe): The pandas dataframe of imported data.
    """
    df = pd.read_csv(file_path, sep=separator, dtype="string")
    return df

def cleanse_data(df_original):
    """Cleanse data step by step.

    Args:
        df_original (dataframe): The pandas dataframe of original data.

    Returns:
        df_processing (dataframe): The pandas dataframe of clean data.
    """
    df_processing = df_original.copy(deep=True)
    logging.info('- Process column entityName.')
    df_processing = process_entityName(df_processing)
    logging.info('- Process column entityType.')
    df_processing = process_entityType(df_processing)
    logging.info('- Process column registrationNumber.')
    df_processing = process_registrationNumber(df_processing)
    logging.info('- Process column IncorporationDate.')
    df_processing = process_incorporationDate(df_processing)
    logging.info('- Process column CountryCode.')
    df_processing = process_countryCode(df_processing)
    logging.info('- Process column StateCode.')
    df_processing = process_stateCode(df_processing)
    logging.info('- Process column Status.')
    df_processing = process_status(df_processing)
    logging.info('- Process column Industry.')
    df_processing = process_industry(df_processing)
    logging.info('- Process column ContactEmail.')
    df_processing = process_contactEmail(df_processing)
    logging.info('- Process column LastUpdate.')
    df_processing = process_lastUpdate(df_processing)
    df_processing["cleanse_reject"] = df_processing[[
        "EntityName_reject",
        "EntityType_reject",
        "RegistrationNumber_reject",
        "IncorporationDate_reject",
        "CountryCode_reject",
        "StateCode_reject",
        "Status_reject",
        "Industry_reject",
        "ContactEmail_reject",
        "LastUpdate_reject"
        ]].all(axis=1)
    return df_processing

def process_entityName(df_processing):
    """Process column EntityName.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "EntityName" not in df_processing.columns:
        logging.error('-- Column "EntityName" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["EntityName"] = df_processing["EntityName"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Validate EntityName contains value or not, reject when it is fail
    df_processing["EntityName_reject"] = df_processing["EntityName"].apply(lambda x: True if x is pd.NA or x == "" or x == " " else False)
    return df_processing

def process_entityType(df_processing):
    """Process column EntityType.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "EntityType" not in df_processing.columns:
        logging.error('-- Column "EntityType" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["EntityType"] = df_processing["EntityType"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Uppercase the first letter and lowercase the remaining letters
    df_processing["EntityType"] = df_processing["EntityType"].apply(lambda x: x[0].upper() + x[1:].lower() if x is not pd.NA else x).astype("string")
    # Validate EntityType as expected value or not, reject when it is fail
    df_processing["EntityType_reject"] = df_processing["EntityType"].apply(lambda x: True if x is pd.NA or x not in LIST_ENTITY_TYPE else False)
    return df_processing

def process_registrationNumber(df_processing):
    """Process column RegistrationNumber.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "RegistrationNumber" not in df_processing.columns:
        logging.error('-- Column "RegistrationNumber" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["RegistrationNumber"] = df_processing["RegistrationNumber"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Uppercase all letters
    df_processing["RegistrationNumber"] = df_processing["RegistrationNumber"].apply(lambda x: x.upper() if x is not pd.NA else x).astype("string")
    # Validate RegistrationNumber as expected format or not, reject when it is fail
    df_processing["RegistrationNumber_reject"] = df_processing["RegistrationNumber"].apply(lambda x: True if x is not pd.NA and re.fullmatch(REGEX_PATTERN_REGISTRATION_NUMBER, x) is None else False)
    return df_processing

def process_incorporationDate(df_processing):
    """Process column IncorporationDate.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "IncorporationDate" not in df_processing.columns:
        logging.error('-- Column "IncorporationDate" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["IncorporationDate"] = df_processing["IncorporationDate"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Revise date format if necessary
    df_processing["IncorporationDate"] = df_processing["IncorporationDate"].apply(lambda x: revise_date_format(x) if x is not pd.NA else x).astype("string")
    # Validate IncorporationDate as expected format or not, reject when it is fail
    df_processing["IncorporationDate_reject"] = df_processing["IncorporationDate"].apply(lambda x: True if x is not pd.NA and re.fullmatch(REGEX_PATTERN_DATE_FORMAT, x) is None else False)
    return df_processing

def revise_date_format(input_str):
    """Revise the date format of the input string. Put MM/DD/YY as the highest priority due to largest usage in sample data.

    Args:
        input_str (string): Input string with date format

    Returns:
        (string): Output string with align date format
    """
    list_date_format = [
        {
            "code": "%m/%d/%y",
            "debug_message": "MM/DD/YY"
        },
        {
            "code": "%m/%d/%Y",
            "debug_message": "MM/DD/YYYY"
        },
        {
            "code": "%d/%m/%y",
            "debug_message": "DD/MM/YY"
        },
        {
            "code": "%d/%m/%Y",
            "debug_message": "DD/MM/YYYY"
        },
        {
            "code": "%m-%d-%y",
            "debug_message": "MM-DD-YY"
        },
        {
            "code": "%m-%d-%Y",
            "debug_message": "MM-DD-YYYY"
        },
        {
            "code": "%Y-%m-%d",
            "debug_message": "YYYY-MM-DD"
        },
        {
            "code": "%d-%b-%y",
            "debug_message": "DD-MMM-YY"
        },
    ]
    for item in list_date_format:
        try:
            temp = date.strptime(input_str, item["code"])
            return temp.strftime(DATE_FORMAT_CODE_OUTPUT)
        except ValueError:
            logging.debug(f'-- string {input_str} is not in the date format {item["debug_message"]}.')
    return input_str

def process_countryCode(df_processing):
    """Process column CountryCode.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "Country" not in df_processing.columns:
        logging.error('-- Column "Country" is missed in CSV data.')
    if "CountryCode" not in df_processing.columns:
        logging.error('-- Column "CountryCode" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace and uppercase the whole string
    df_processing["CountryCode_revised"] = df_processing["CountryCode"].apply(lambda x: x.strip().upper() if x is not pd.NA else x).astype("string")
    # Extract the first two letters if correct format is found
    df_processing["CountryCode_revised"] = df_processing["CountryCode_revised"].apply(lambda x: x[0:1] if x is not pd.NA and re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT + r"(?:-.+)?", x) is not None else x).astype("string")
    # Check the CountryCode is valid or not, remove if it not valid
    df_processing["CountryCode_revised"] = df_processing["CountryCode_revised"].apply(lambda x: pycountry.countries.get(alpha_2=x).alpha_2 if x is not pd.NA and pycountry.countries.get(alpha_2=x) is not None else pd.NA).astype("string")
    # Use Country to provide CountryCode if CountryCode is missing
    df_processing["CountryCode_revised"] = df_processing.apply(lambda x: convert_country_name_to_country_code(x["Country"]) if x["CountryCode_revised"] is pd.NA and "Country" in x.keys() and x["Country"] is not pd.NA else x["CountryCode_revised"], axis=1).astype("string")
    # Validate CountryCode as expected format or not, reject when it is fail
    df_processing["CountryCode_reject"] = df_processing["CountryCode_revised"].apply(lambda x: True if x is pd.NA or (x is not pd.NA and re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT, x) is None) else False)
    return df_processing

def convert_country_name_to_country_code(input_str):
    """Convert the input_str, which is expected as country name, to country code.

    Args:
        input_str (string): Input string expected as country name

    Returns:
        (string): Output string as country code
    """
    try:
        results = pycountry.countries.search_fuzzy(input_str)
        return results[0].alpha_2
    except LookupError:
        logging.debug(f'-- string {input_str} is not a valid country name.')
        return input_str

def process_stateCode(df_processing):
    """Process column StateCode.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "CountryCode" not in df_processing.columns:
        logging.error('-- Column "CountryCode" is missed in CSV data.')
    if "State" not in df_processing.columns:
        logging.error('-- Column "State" is missed in CSV data.')
    if "StateCode" not in df_processing.columns:
        logging.error('-- Column "StateCode" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace and uppercase the whole string
    df_processing["StateCode_revised"] = df_processing["StateCode"].apply(lambda x: x.strip().upper() if x is not pd.NA else x).astype("string")
    # Remove if StateCode is same as CountryCode
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: pd.NA if x["StateCode_revised"] is not pd.NA and "CountryCode_revised" in x.keys() and x["CountryCode_revised"] is not pd.NA and x["StateCode_revised"] == x["CountryCode_revised"] else x["StateCode_revised"], axis=1).astype("string")
    # Extract the subdivison code from CountryCode if CountryCode is in specific format
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT + r"-(.+)", x["CountryCode"]).group(0) if x["StateCode_revised"] is pd.NA and "CountryCode" in x.keys() and x["CountryCode"] is not pd.NA and re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT + r"-(.+)", x["CountryCode"]) is not None else x["StateCode_revised"], axis=1).astype("string")
    # Check the StateCode is valid or not, remove if it not valid
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: pycountry.subdivisions.get(code=x["CountryCode_revised"] + "-" + x["StateCode_revised"]).code.split("-")[1] if x["StateCode_revised"] is not pd.NA and "CountryCode_revised" in x.keys() and x["CountryCode_revised"] is not pd.NA and x["StateCode_revised"] != x["CountryCode_revised"] and pycountry.subdivisions.get(code=x["CountryCode_revised"] + "-" + x["StateCode_revised"]) is not None else pd.NA, axis=1).astype("string")
    # Use State to provide StateCode if StateCode is missing
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: convert_state_name_to_state_code(x["State"], x["CountryCode_revised"] if "CountryCode_revised" in x.keys() else pd.NA) if x["StateCode_revised"] is pd.NA and "State" in x.keys() and x["State"] is not pd.NA else x["StateCode_revised"], axis=1).astype("string")
    # Invalid value is removed and missing value is allowed, thus none of the records will be rejected due to StateCode
    df_processing["StateCode_reject"] = False
    return df_processing

def convert_state_name_to_state_code(input_str, country_code):
    """Convert the input_str, which is expected as state name, to state code.

    Args:
        input_str (string): Input string expected as state name
        country_code (string): Country code to be used as the language code for translation

    Returns:
        (string): Output string as state code
    """
    try:
        results = pycountry.subdivisions.search_fuzzy(input_str)
        return results[0].code.split("-")[1]
    except LookupError:
        logging.debug(f'-- string {input_str} is not a valid state name.')

    # try to search the state name in the language used by the country
    try:
        lang_list = list(pycountry.languages)
        lang_list = [x.alpha_2 for x in lang_list if hasattr(x, "alpha_2")]
        if country_code.lower() in lang_list:
            translator = Translator(to_lang=country_code)
            translation = translator.translate(input_str)
            results = pycountry.subdivisions.search_fuzzy(translation)
            return results[0].code.split("-")[1]
    except LookupError:
        logging.debug(f'-- string {translation} is not a valid state name.')

    return input_str

def process_status(df_processing):
    """Process column Status.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "Status" not in df_processing.columns:
        logging.error('-- Column "Status" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["Status"] = df_processing["Status"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Convert "Y" to "Active", "N" to "Inactive"
    df_processing["Status"] = df_processing["Status"].apply(lambda x: DICT_STATUS_MAPPING.get(x, x) if x is not pd.NA else x).astype("string")
    # Standardize the wordings
    df_processing["Status"] = df_processing["Status"].apply(lambda x: re.fullmatch(rf"({"|".join(LIST_STATUS)}).*", x).group(1) if x is not pd.NA and re.fullmatch(rf"({"|".join(LIST_STATUS)}).*", x) is not None else x).astype("string")
    # Validate Status as expected value or not, reject when it is fail
    df_processing["Status_reject"] = df_processing["Status"].apply(lambda x: True if x is pd.NA or (x is not pd.NA and x not in LIST_STATUS) else False)
    return df_processing

def process_industry(df_processing):
    """Process column Industry.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "Industry" not in df_processing.columns:
        logging.error('-- Column "Industry" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["Industry"] = df_processing["Industry"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Remove "NULL"
    df_processing["Industry"] = df_processing["Industry"].apply(lambda x: pd.NA if x is not pd.NA and x == "NULL" else x).astype("string")
    # Standardize capitalization
    df_processing["Industry"] = df_processing["Industry"].apply(lambda x: " ".join([y[0].upper() + y[1:].lower() for y in x.split(" ")]) if x is not pd.NA else x).astype("string")
    # None of the records will be rejected due to Industry
    df_processing["Industry_reject"] = False
    return df_processing

def process_contactEmail(df_processing):
    """Process column ContactEmail.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "ContactEmail" not in df_processing.columns:
        logging.error('-- Column "ContactEmail" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["ContactEmail"] = df_processing["ContactEmail"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Validate ContactEmail as expected format or not, reject when it is fail
    df_processing["ContactEmail_reject"] = df_processing["ContactEmail"].apply(lambda x: True if x is not pd.NA and re.fullmatch(r".+@.+", x) is None else False)
    return df_processing


def process_lastUpdate(df_processing):
    """Process column LastUpdate.

    Args:
        df_processing (dataframe): The pandas dataframe of processing data.

    Returns:
        df_processing (dataframe): The pandas dataframe of processing data.
    """
    if "LastUpdate" not in df_processing.columns:
        logging.error('-- Column "LastUpdate" is missed in CSV data.')
        raise Exception("CSV data has missed some columns")
    # Remove whitespace
    df_processing["LastUpdate"] = df_processing["LastUpdate"].apply(lambda x: x.strip() if x is not pd.NA else x).astype("string")
    # Revise date format if necessary
    df_processing["LastUpdate"] = df_processing["LastUpdate"].apply(lambda x: revise_date_format(x) if x is not pd.NA else x).astype("string")
    # Validate LastUpdate as expected format or not, reject when it is fail
    df_processing["LastUpdate_reject"] = df_processing["LastUpdate"].apply(lambda x: True if x is not pd.NA and re.fullmatch(REGEX_PATTERN_DATE_FORMAT, x) is None else False)
    return df_processing

def deduplicate_records(df_in):
    """Deduplicate records for the input dataframe. Output as two dataframes, deduplicated entities and rejected entities due to duplication with other different information

    Args:
        df_in (dataframe): The pandas dataframe needed to be deduplicated.

    Returns:
        df_deduplicate (dataframe): The pandas dataframe which is deduplicated.
        df_duplicate_reject (dataframe): The pandas dataframe which is duplicate in EntityName and EntityType but other information is different.
    """
    df_in["duplicate_candidate"] = df_in[["EntityName", "EntityType"]].duplicated(keep=False)
    df_duplicate_reject = pd.DataFrame(columns=df_in.columns).astype(df_in.dtypes)
    logging.info('- Decouple unique records and duplicate candidates.')
    df_deduplicate = df_in[df_in["duplicate_candidate"] == False]
    df_duplicate_candidate = df_in[df_in["duplicate_candidate"] == True]
    logging.info('- Checking all useful columns to decide whether it is duplicate reject case or not for each duplicate candidate group.')
    list_of_df_duplicate_candidate_group = [group for name, group in df_duplicate_candidate.groupby(["EntityName", "EntityType"])]
    for x in list_of_df_duplicate_candidate_group:
        if x[[item[0] for item in LIST_SCHEMA_MAPPING if item[0] != "EntityID"]].duplicated(keep=False).all(axis=0):
            df_deduplicate = pd.concat([df_deduplicate, x.drop_duplicates(subset=[item[0] for item in LIST_SCHEMA_MAPPING if item[0] != "EntityID"])], ignore_index=True)
        else:
            df_duplicate_reject = pd.concat([df_duplicate_reject, x], ignore_index=True)
    return df_deduplicate, df_duplicate_reject

def validate_business_rules(df_in):
    """Validate the input dataframe with business rule.

    Args:
        df_in (dataframe): The pandas dataframe needed to be validated.

    Returns:
        df_processing (dataframe): The pandas dataframe which is validated against business rules.
    """
    df_processing = df_in.copy(deep=True)
    df_processing["business_rules_reject"] = False
    # Validate IncorporationDate has value or not, reject when it is fail
    logging.info('- Validate IncorporationDate.')
    df_processing["business_rules_reject"] = df_processing.apply(lambda x: True if x["IncorporationDate"] is pd.NA else x["business_rules_reject"], axis=1)
    return df_processing

def transform_fields(df_in):
    """Transform fields to fit MySQL schema.

    Args:
        df_in (dataframe): The pandas dataframe needed to be transformed.

    Returns:
        df_out (dataframe): The pandas dataframe with transformation.
    """
    df_processing = df_in.copy(deep=True)
    for item in LIST_SCHEMA_MAPPING:
        logging.info(f'- Rename {item[0]} as {item[1]} and convert as {item[2]} type.')
        df_processing.rename(columns={item[0]: item[1]}, inplace=True)
        if item[2] == "int":
            df_processing[item[1]] = df_processing[item[1]].astype("int")
        if item[2] == "date":
            df_processing[item[1]] = pd.to_datetime(df_processing[item[1]])
        df_processing[item[1]] = df_processing[item[1]].replace({pd.NA: None})
    df_out = df_processing[[x[1] for x in LIST_SCHEMA_MAPPING]]
    return df_out

def load_to_MySQL(dict_connection_credential, df_upload):
    """Load data to MySQL database.

    Args:
        dict_connection_credential (dict): contain credential to connect MySQL database
        df_upload (dataframe): The pandas dataframe to be uploaded to MySQL database.

    Returns:
        affected_rows (int): Number of affected rows.
    """
    try:
        # Establish the connection to the MySQL server
        cnx = mysql.connector.connect(
            host=dict_connection_credential["HOST"],
            port=int(dict_connection_credential["PORT"]),
            user=dict_connection_credential["USER"],
            password=dict_connection_credential["PASSWORD"],
            database=dict_connection_credential["SCHEMA"]
        )
        cur = cnx.cursor()

        # Create table if not exist.
        cur.execute(QUERY_CREATE_TABLE_ENTITIES.replace('<TABLE_NAME>', dict_connection_credential["TABLE_ENTITIES"]))
        logging.info(f'- Table "{dict_connection_credential["TABLE_ENTITIES"]}" ensured to exist (created if not present)')

        affected_rows = None

        # Insert or update the data
        cur.executemany(QUERY_INSERT_UPDATE_ENTITY.replace('<TABLE_NAME>', dict_connection_credential["TABLE_ENTITIES"]), list(df_upload.itertuples(index=False, name=None)))

        # Commit the changes to the database
        cnx.commit()

        affected_rows = cur.rowcount
        logging.info(f'- {affected_rows} rows affected (inserted or updated).')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error('- Something is wrong with user name or password!')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error(f'- Database {dict_connection_credential["SCHEMA"]} does not exist!')
        else:
            print(err)
    finally:
        # Close the cursor and connection
        if cnx.is_connected():
            cur.close()
            cnx.close()
            logging.info('- MySQL connection is closed.')
    return affected_rows

if __name__ == "__main__":
    # Ingest CSV data
    logging.info('Ingest CSV data.')
    df_source = ingest_csv(CSV_PATH, CSV_DATA_SEPARATOR)
    processed_rows = len(df_source)
    # Cleanse data
    logging.info('Cleanse data.')
    df_cleanse = cleanse_data(df_source)
    df_cleanse_accept = df_cleanse[df_cleanse["cleanse_reject"]  == False]
    df_cleanse_reject = df_cleanse[df_cleanse["cleanse_reject"]  == True]
    # Deduplicate records
    logging.info('Deduplicate records.')
    df_deduplicate, df_duplicate_reject = deduplicate_records(df_cleanse_accept)
    # Validate data against business rules
    logging.info('Validate against business rules.')
    df_business_rules = validate_business_rules(df_deduplicate)
    df_business_rules_accept = df_business_rules[df_business_rules["business_rules_reject"] == False]
    df_business_rules_reject = df_business_rules[df_business_rules["business_rules_reject"] == True]
    # Transform fields to fit MySQL schema
    logging.info('Transform to fit MySQL schema.')
    df_fit_schema = transform_fields(df_business_rules_accept)
    # Load clean data into MySQL tables
    logging.info('Load to MySQL tables.')
    uploaded_rows = load_to_MySQL(MYSQL_CONNECTION_CREDENTIAL, df_fit_schema)
    # Quarantine rejected/problematic records for manual review
    logging.info('Quarantine rejected/problematic records.')

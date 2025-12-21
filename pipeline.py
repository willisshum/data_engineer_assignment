from dotenv import load_dotenv
import os
import logging
import pandas as pd
import re
from datetime import date
import pycountry
from translate import Translator

from reference_value import LIST_ENTITY_TYPE, REGEX_PATTERN_REGISTRATION_NUMBER, REGEX_PATTERN_DATE_FORMAT, DATE_FORMAT_CODE_OUTPUT, REGEX_PATTERN_COUNTRY_CODE_OUTPUT, LIST_STATUS, DICT_STATUS_MAPPING

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
CSV_DATA_SEPARATOR = os.environ.get("CSV_DATA_SEPARATOR")

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
    df_processing["reject"] = df_processing[[
        "EntityName_reject",
        "EntityType_reject",
        "RegistrationNumber_reject",
        "IncorporationDate_reject",
        "CountryCode_reject",
        "StateCode_reject",
        "Status_reject",
        "Industry_reject",
        "ContactEmail_reject"
        ]].all()
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
    df_processing["EntityName"] = df_processing["EntityName"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
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
    df_processing["EntityType"] = df_processing["EntityType"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
    # Uppercase the first letter and lowercase the remaining letters
    df_processing["EntityType"] = df_processing["EntityType"].apply(lambda x: x[0].upper() + x[1:].lower() if x is not pd.NA else x, by_row='compat').astype("string")
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
    df_processing["RegistrationNumber"] = df_processing["RegistrationNumber"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
    # Uppercase all letters
    df_processing["RegistrationNumber"] = df_processing["RegistrationNumber"].apply(lambda x: x.upper() if x is not pd.NA else x, by_row='compat').astype("string")
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
    df_processing["IncorporationDate"] = df_processing["IncorporationDate"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
    # Revise date format if necessary
    df_processing["IncorporationDate"] = df_processing["IncorporationDate"].apply(lambda x: revise_date_format(x) if x is not pd.NA else x, by_row='compat').astype("string")
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
    df_processing["CountryCode_revised"] = df_processing["CountryCode"].apply(lambda x: x.strip().upper() if x is not pd.NA else x, by_row='compat').astype("string")
    # Extract the first two letters if correct format is found
    df_processing["CountryCode_revised"] = df_processing["CountryCode_revised"].apply(lambda x: x[0:1] if x is not pd.NA and re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT + r"(?:-.+)?", x) is not None else x, by_row='compat').astype("string")
    # Check the CountryCode is valid or not, remove if it not valid
    df_processing["CountryCode_revised"] = df_processing["CountryCode_revised"].apply(lambda x: pycountry.countries.get(alpha_2=x).alpha_2 if x is not pd.NA and pycountry.countries.get(alpha_2=x) is not None else pd.NA, by_row='compat').astype("string")
    # Use Country to provide CountryCode if CountryCode is missing
    df_processing["CountryCode_revised"] = df_processing.apply(lambda x: convert_country_name_to_country_code(x["Country"]) if x["CountryCode_revised"] is pd.NA and "Country" in x.keys() and x["Country"] is not pd.NA else x["CountryCode_revised"], by_row='compat', axis=1).astype("string")
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
    df_processing["StateCode_revised"] = df_processing["StateCode"].apply(lambda x: x.strip().upper() if x is not pd.NA else x, by_row='compat').astype("string")
    # Remove if StateCode is same as CountryCode
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: pd.NA if x["StateCode_revised"] is not pd.NA and "CountryCode_revised" in x.keys() and x["CountryCode_revised"] is not pd.NA and x["StateCode_revised"] == x["CountryCode_revised"] else x["StateCode_revised"], by_row='compat', axis=1).astype("string")
    # Extract the subdivison code from CountryCode if CountryCode is in specific format
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT + r"-(.+)", x["CountryCode"]).group(0) if x["StateCode_revised"] is pd.NA and "CountryCode" in x.keys() and x["CountryCode"] is not pd.NA and re.fullmatch(REGEX_PATTERN_COUNTRY_CODE_OUTPUT + r"-(.+)", x["CountryCode"]) is not None else x["StateCode_revised"], by_row='compat', axis=1).astype("string")
    # Check the StateCode is valid or not, remove if it not valid
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: pycountry.subdivisions.get(code=x["CountryCode_revised"] + "-" + x["StateCode_revised"]).code.split("-")[1] if x["StateCode_revised"] is not pd.NA and "CountryCode_revised" in x.keys() and x["CountryCode_revised"] is not pd.NA and x["StateCode_revised"] != x["CountryCode_revised"] and pycountry.subdivisions.get(code=x["CountryCode_revised"] + "-" + x["StateCode_revised"]) is not None else pd.NA, by_row='compat', axis=1).astype("string")
    # Use State to provide StateCode if StateCode is missing
    df_processing["StateCode_revised"] = df_processing.apply(lambda x: convert_state_name_to_state_code(x["State"], x["CountryCode_revised"] if "CountryCode_revised" in x.keys() else pd.NA) if x["StateCode_revised"] is pd.NA and "State" in x.keys() and x["State"] is not pd.NA else x["StateCode_revised"], by_row='compat', axis=1).astype("string")
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
    df_processing["Status"] = df_processing["Status"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
    # Convert "Y" to "Active", "N" to "Inactive"
    df_processing["Status"] = df_processing["Status"].apply(lambda x: DICT_STATUS_MAPPING.get(x, x) if x is not pd.NA else x, by_row='compat').astype("string")
    # Standardize the wordings
    df_processing["Status"] = df_processing["Status"].apply(lambda x: re.fullmatch(rf"({"|".join(LIST_STATUS)}).*", x).group(1) if x is not pd.NA and re.fullmatch(rf"({"|".join(LIST_STATUS)}).*", x) is not None else x, by_row='compat').astype("string")
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
    df_processing["Industry"] = df_processing["Industry"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
    # Remove "NULL"
    df_processing["Industry"] = df_processing["Industry"].apply(lambda x: pd.NA if x is not pd.NA and x == "NULL" else x, by_row='compat').astype("string")
    # Standardize capitalization
    df_processing["Industry"] = df_processing["Industry"].apply(lambda x: " ".join([y[0].upper() + y[1:].lower() for y in x.split(" ")]) if x is not pd.NA else x, by_row='compat').astype("string")
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
    df_processing["ContactEmail"] = df_processing["ContactEmail"].apply(lambda x: x.strip() if x is not pd.NA else x, by_row='compat').astype("string")
    # Validate ContactEmail as expected format or not, reject when it is fail
    df_processing["ContactEmail_reject"] = df_processing["ContactEmail"].apply(lambda x: True if x is not pd.NA and re.fullmatch(r".+@.+", x) is None else False)
    return df_processing

if __name__ == "__main__":
    # Ingest CSV data
    logging.info('Ingest CSV data.')
    df_source = ingest_csv(CSV_PATH, CSV_DATA_SEPARATOR)
    # Cleanse data
    logging.info('Cleanse data.')
    df_clean = cleanse_data(df_source)
    # Deduplicate records
    logging.info('Deduplicate records.')
    # Validate data against business rules
    logging.info('Validate against business rules.')
    # Transform fields to fit MySQL schema
    logging.info('Transform to fit MySQL schema.')
    # Load clean data into MySQL tables
    logging.info('Load to MySQL tables.')
    # Quarantine rejected/problematic records for manual review
    logging.info('Quarantine rejected/problematic records.')

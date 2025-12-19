from dotenv import load_dotenv
import os
import logging
import pandas as pd
import re

from reference_value import LIST_ENTITY_TYPE, REGEX_PATTERN_REGISTRATION_NUMBER

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
    df_processing["reject"] = df_processing[[
        "EntityName_reject",
        "EntityType_reject",
        "RegistrationNumber_reject"
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
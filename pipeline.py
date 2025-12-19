from dotenv import load_dotenv
import os
import logging
import pandas as pd

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

if __name__ == "__main__":
    # Ingest CSV data
    logging.info('Ingest CSV data.')
    df_source = ingest_csv(CSV_PATH, CSV_DATA_SEPARATOR)
    # Cleanse data
    logging.info('Cleanse data.')
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
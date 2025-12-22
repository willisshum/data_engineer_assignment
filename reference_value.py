LIST_ENTITY_TYPE = [
    "Company",
    "Nonprofit",
    "Partnership",
    "Trust"
]

REGEX_PATTERN_REGISTRATION_NUMBER = r'REG\d{5}'

REGEX_PATTERN_DATE_FORMAT = r'\d{4}-\d{2}-\d{2}'

DATE_FORMAT_CODE_OUTPUT = "%Y-%m-%d"

REGEX_PATTERN_COUNTRY_CODE_OUTPUT = r'[A-Z]{2}'

LIST_STATUS = [
    "Active",
    "Inactive",
    "Pending"
]

DICT_STATUS_MAPPING = {
    "Y": "Active",
    "N": "Inactive"
}

# For each tuple, the first value is CSV column, the second value is MySQL schema, the third value is data type
LIST_SCHEMA_MAPPING = [
    ("EntityName", "entity_name", "string"),
    ("EntityType", "entity_type", "string"),
    ("RegistrationNumber", "registration_number", "string"),
    ("IncorporationDate", "incorporation_date", "date"),
    ("CountryCode_revised", "country_code", "string"),
    ("StateCode_revised", "state_code", "string"),
    ("Status", "status", "string"),
    ("Industry", "industry", "string"),
    ("ContactEmail", "contact_email", "string"),
    ("LastUpdate", "last_update", "date")
]
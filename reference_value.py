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

# For each tuple, the first value is CSV column, the second value is MySQL schema
LIST_SCHEMA_MAPPING = [
    ("EntityName", "entity_name"),
    ("EntityType", "entity_type"),
    ("RegistrationNumber", "registration_number"),
    ("IncorporationDate", "incorporation_date"),
    ("CountryCode_revised", "country_code"),
    ("StateCode_revised", "state_code"),
    ("Status", "status"),
    ("Industry", "industry"),
    ("ContactEmail", "contact_email"),
    ("LastUpdate", "last_update")
]
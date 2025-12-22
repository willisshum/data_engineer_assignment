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
    ("EntityID", "entity_id", "int"),
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

QUERY_CREATE_TABLE_ENTITIES = """
CREATE TABLE IF NOT EXISTS <TABLE_NAME> (
    entity_id INT PRIMARY KEY AUTO_INCREMENT,
    entity_name VARCHAR(150) NOT NULL,
    entity_type VARCHAR(30),
    registration_number VARCHAR(50),
    incorporation_date DATE,
    country_code VARCHAR(3),
    state_code VARCHAR(50),
    status VARCHAR(30),
    industry VARCHAR(100),
    contact_email VARCHAR(100),
    last_update DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

QUERY_INSERT_UPDATE_ENTITY = """
INSERT INTO <TABLE_NAME> (
    entity_id,
    entity_name,
    entity_type,
    registration_number,
    incorporation_date,
    country_code,
    state_code,
    status,
    industry,
    contact_email,
    last_update
)
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s,
    %s
)
ON DUPLICATE KEY UPDATE
    entity_name = VALUES(entity_name),
    entity_type = VALUES(entity_type),
    registration_number = VALUES(registration_number),
    incorporation_date = VALUES(incorporation_date),
    country_code = VALUES(country_code),
    state_code = VALUES(state_code),
    status = VALUES(status),
    industry = VALUES(industry),
    contact_email = VALUES(contact_email),
    last_update = VALUES(last_update);
"""
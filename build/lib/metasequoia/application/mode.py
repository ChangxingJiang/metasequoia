import enum


class ApplicationMode(enum.Enum):
    DEV = "DEV"
    TEST = "TEST"
    PRE = "PRE"
    PROD = "PROD"

import enum


class ApplicationMode(enum.Enum):
    DEV = "DEV"
    TEST = "TEST"
    PRE = "PRE"
    PROD = "PROD"

    @property
    def is_dev(self) -> bool:
        return self.name == "DEV"

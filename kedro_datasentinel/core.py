from enum import Enum


class KedroDataSentinelError(Exception):
    pass


class DataSentinelConfigError(KedroDataSentinelError):
    pass


class DataValidationConfigError(KedroDataSentinelError):
    pass


class RuleNotImplementedError(KedroDataSentinelError):
    pass


class Mode(Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    BOTH = "BOTH"


class Event(Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

from enum import Enum, auto


class MaterializeIncrementalType(Enum):
    BIGQUERY = auto()
    REDSHIFT = auto()

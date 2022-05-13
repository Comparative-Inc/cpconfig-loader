from marshmallow_dataclass import class_schema
from yaml import SafeLoader, load

from cpconfig.ds import CpConfig


def load_cpconfig_yaml_file(path: str) -> CpConfig:
    """Load CpConfig from a YAML file"""
    schema = class_schema(CpConfig)
    return schema().load(load(open(path, "r").read(), Loader=SafeLoader))

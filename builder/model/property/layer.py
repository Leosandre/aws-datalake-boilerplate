from enum import Enum


class DatalakeLayer(Enum):
    RAW = "raw"
    TRUSTED = "trusted"
    SERVICE = "service"

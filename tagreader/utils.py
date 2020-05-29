import enum
import logging
import winreg
import pandas as pd


def find_registry_key(base_key, search_key_name):
    search_key_name = search_key_name.lower()
    if base_key is not None:
        num_keys, _, _ = winreg.QueryInfoKey(base_key)
        for i in range(0, num_keys):
            key_name = winreg.EnumKey(base_key, i)
            if key_name.lower() == search_key_name:
                return winreg.OpenKey(base_key, key_name)
            else:
                key = find_registry_key(
                    winreg.OpenKey(base_key, key_name), search_key_name
                )
            if key is not None:
                return key
    return None


def find_registry_key_from_name(base_key, search_key_name):
    search_key_name = search_key_name.lower()
    num_keys, _, _ = winreg.QueryInfoKey(base_key)
    key = key_string = None
    for i in range(0, num_keys):
        try:
            key_string = winreg.EnumKey(base_key, i)
            key = winreg.OpenKey(base_key, key_string)
            _, num_vals, _ = winreg.QueryInfoKey(key)
            if num_vals > 0:
                (_, key_name, _) = winreg.EnumValue(key, 0)
                if str(key_name).lower() == search_key_name:
                    break
        except Exception as err:
            logging.error("{}: {}".format(i, err))
    return key, key_string


def datestr_to_datetime(date_stamp, tz="Europe/Oslo"):
    date_stamp = (
        pd.to_datetime(date_stamp, dayfirst=True).tz_localize(tz)
        if isinstance(date_stamp, str)
        else date_stamp
    )
    if not date_stamp.tzinfo:
        date_stamp = date_stamp.tz_localize(tz)
    else:
        date_stamp = date_stamp.tz_convert(tz)
    return date_stamp


class ReaderType(enum.IntEnum):
    """Enumerates available types of data to read.

    For members with more than one name per value, the first member (the
    original) needs to be untouched since it may be used as back-reference
    (specifically for cache hierarchies).
    """

    RAW = SAMPLED = enum.auto()  # Raw sampled data
    SHAPEPRESERVING = enum.auto()  # Minimum data points while preserving shape
    INT = INTERPOLATE = INTERPOLATED = enum.auto()  # Interpolated data
    MIN = MINIMUM = enum.auto()  # Min value
    MAX = MAXIMUM = enum.auto()  # Max value
    AVG = AVERAGE = AVERAGED = enum.auto()  # Averaged data
    VAR = VARIANCE = enum.auto()  # Variance of data
    STD = STDDEV = enum.auto()  # Standard deviation of data
    RNG = RANGE = enum.auto()  # Range of data
    COUNT = enum.auto()  # Number of data points
    GOOD = enum.auto()  # Number of good data points
    BAD = NOTGOOD = enum.auto()  # Number of not good data points
    TOTAL = enum.auto()  # Number of total data
    SUM = enum.auto()  # Sum of data
    SNAPSHOT = FINAL = LAST = enum.auto()  # Last sampled value

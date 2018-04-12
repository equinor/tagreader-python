import logging
import winreg
import pandas as pd


def __init__():
    __all__ == ['find_registry_key',
                'find_registry_key_from_name',
                'datestr_to_datetime']


def find_registry_key(base_key, search_key_name):
    search_key_name = search_key_name.lower()
    if base_key is not None:
        num_keys, _, _ = winreg.QueryInfoKey(base_key)
        for i in range(0, num_keys):
            key_name = winreg.EnumKey(base_key, i)
            if key_name.lower() == search_key_name:
                return winreg.OpenKey(base_key, key_name)
            else:
                key = find_registry_key(winreg.OpenKey(base_key, key_name), search_key_name)
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
            logging.error('{}: {}'.format(i, err))
    return key, key_string


def datestr_to_datetime(date_stamp):
    return pd.to_datetime(date_stamp, dayfirst=True) if isinstance(date_stamp, str) else date_stamp

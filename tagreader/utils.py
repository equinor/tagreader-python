import enum
import logging
import warnings
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


def ensure_datetime_with_tz(date_stamp, tz="Europe/Oslo"):
    if isinstance(date_stamp, str):
        date_stamp = pd.to_datetime(date_stamp, dayfirst=True)

    if not date_stamp.tzinfo:
        date_stamp = date_stamp.tz_localize(tz)
    return date_stamp


def urljoin(*args):
    """Joins components of URL. Ensures slashes are inserted or removed where
    needed, and does not strip trailing slash of last element.

    Arguments:
        str
    Returns:
        str -- Generated URL
    """
    trailing_slash = "/" if args[-1].endswith("/") else ""
    return "/".join(map(lambda x: str(x).strip("/"), args)) + trailing_slash


class ReaderType(enum.IntEnum):
    """Enumerates available types of data to read.

    For members with more than one name per value, the first member (the
    original) needs to be untouched since it may be used as back-reference
    (specifically for cache hierarchies).
    """

    RAW = SAMPLED = ACTUAL = enum.auto()  # Raw sampled data
    SHAPEPRESERVING = BESTFIT = enum.auto()  # Minimum data points for preserving shape
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


def add_statoil_root_certificate(noisy=True):
    """This is a utility function for Equinor employees on Equinor machines.

    The function searches for the Statoil Root certificate in the Windows
    cert store and imports it to the cacert bundle. Does nothing if not
    running on Equinor host.

    This needs to be repeated after updating the cacert module.

    Returns:
        bool: True if function completes successfully
    """
    import ssl
    import certifi
    import hashlib

    STATOIL_ROOT_PEM_HASH = "ce7bb185ab908d2fea28c7d097841d9d5bbf2c76"

    if noisy:
        print("Scanning CA certs in Windows cert store", end="")
    found = False
    for cert in ssl.enum_certificates("CA"):
        if noisy:
            print(".", end="")
        der = cert[0]
        if hashlib.sha1(der).hexdigest() == STATOIL_ROOT_PEM_HASH:
            found = True
            if noisy:
                print(" found it!")
            pem = ssl.DER_cert_to_PEM_cert(cert[0])
            if pem in certifi.contents():
                if noisy:
                    print("Certificate already exists in certifi store. Nothing to do.")
                break
            if noisy:
                print("Writing certificate to certifi store.")
            cafile = certifi.where()
            with open(cafile, "ab") as f:
                f.write(bytes(pem, "ascii"))
            if noisy:
                print("Completed")
            break

    if not found:
        warnings.warn("Unable to locate Statoil root certificate on this host.")
        return False

    return True


def is_equinor() -> bool:
    """Determines whether code is running on an Equinor host

    Finds host's domain in Windows Registry at
    HKLM\\SYSTEM\\ControlSet001\\Services\\Tcpip\\Parameters\\Domain

    Returns:
        bool: True if Equnor
    """
    with winreg.OpenKey(
        winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\ControlSet001\Services\Tcpip\Parameters"
    ) as key:
        domain = winreg.QueryValueEx(key, "Domain")
    if "statoil" in domain[0]:
        return True
    return False

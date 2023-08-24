import enum
import logging
import platform
import warnings
from datetime import datetime, tzinfo
from enum import Enum
from typing import Union

import pandas as pd
import pytz

from tagreader.logger import logger


def is_windows() -> bool:
    return platform.system() == "Windows"


def is_mac() -> bool:
    return platform.system() == "Darwin"


def is_linux() -> bool:
    return platform.system() == "Linux"


if is_windows():
    import winreg


if is_mac():
    import socket
    import subprocess


def find_registry_key(base_key, search_key_name: str):
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


def find_registry_key_from_name(base_key, search_key_name: str):
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


def convert_to_pydatetime(date_stamp: Union[datetime, str, pd.Timestamp]) -> datetime:
    if isinstance(date_stamp, datetime):
        return date_stamp
    elif isinstance(date_stamp, pd.Timestamp):
        return date_stamp.to_pydatetime()
    else:
        try:
            return pd.to_datetime(str(date_stamp), format="ISO8601").to_pydatetime()
        except ValueError:
            return pd.to_datetime(str(date_stamp), dayfirst=True).to_pydatetime()


def ensure_datetime_with_tz(
    date_stamp: Union[datetime, str, pd.Timestamp],
    tz: tzinfo = pytz.timezone("Europe/Oslo"),
) -> datetime:
    date_stamp = convert_to_pydatetime(date_stamp)

    if not date_stamp.tzinfo:
        date_stamp = tz.localize(date_stamp)

    return date_stamp


def urljoin(*args) -> str:
    """
    Joins components of URL. Ensures slashes are inserted or removed where
    needed, and does not strip trailing slash of last element.

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


def add_statoil_root_certificate() -> bool:
    """
    This is a utility function for Equinor employees on Equinor managed machines.

    The function searches for the Equinor Root certificate in the
    cert store and imports it to the cacert bundle. Does nothing if not
    running on Equinor host.

    This needs to be repeated after updating the cacert module.

    Returns:
        bool: True if function completes successfully
    """
    import hashlib
    import ssl

    import certifi

    STATOIL_ROOT_PEM_HASH = "ce7bb185ab908d2fea28c7d097841d9d5bbf2c76"

    found = False
    der = None

    if is_linux():
        return True
    elif is_windows():
        logger.debug("Scanning CA certificate in Windows cert store", end="")
        for cert in ssl.enum_certificates("CA"):
            der = cert[0]
            if hashlib.sha1(der).hexdigest() == STATOIL_ROOT_PEM_HASH:
                found = True
                logger.debug("CA certificate found!")
                break
    elif is_mac():
        import subprocess

        macos_ca_certs = subprocess.run(
            ["security", "find-certificate", "-a", "-c", "Statoil Root CA", "-Z"],
            stdout=subprocess.PIPE,
        ).stdout

        if STATOIL_ROOT_PEM_HASH.upper() in str(macos_ca_certs).upper():
            c = get_macos_equinor_certificates()
            for cert in c:
                if hashlib.sha1(cert).hexdigest() == STATOIL_ROOT_PEM_HASH:
                    der = cert
                    found = True
                    break

    if found and der:
        pem = ssl.DER_cert_to_PEM_cert(der)
        if pem in certifi.contents():
            logger.debug(
                "CA Certificate already exists in certifi store. Nothing to do."
            )
        else:
            logger.debug("Writing certificate to certifi store.")
            ca_file = certifi.where()
            with open(ca_file, "ab") as f:
                f.write(bytes(pem, "ascii"))
            logger.debug("Completed")
    else:
        warnings.warn("Unable to locate root certificate on this host.")

    return found


def get_macos_equinor_certificates():
    import ssl
    import tempfile

    ctx = ssl.create_default_context()
    macos_ca_certs = subprocess.run(
        ["security", "find-certificate", "-a", "-c", "Statoil Root CA", "-p"],
        stdout=subprocess.PIPE,
    ).stdout
    with tempfile.NamedTemporaryFile("w+b") as tmp_file:
        tmp_file.write(macos_ca_certs)
        ctx.load_verify_locations(tmp_file.name)

    return ctx.get_ca_certs(binary_form=True)


def is_equinor() -> bool:
    """Determines whether code is running on an Equinor host

    If Windows host:
    Finds host's domain in Windows Registry at
    HKLM\\SYSTEM\\ControlSet001\\Services\\Tcpip\\Parameters\\Domain
    If mac os host:
    Finds statoil.net as AD hostname in certificates
    If Linux host:
    Checks whether statoil.no is search domain

    Returns:
        bool: True if Equinor
    """
    if is_windows():
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\ControlSet001\Services\Tcpip\Parameters"
        ) as key:
            domain = winreg.QueryValueEx(key, "Domain")
        if "statoil" in domain[0]:
            return True
    elif is_mac():
        s = subprocess.run(
            ["security", "find-certificate", "-a", "-c" "client.statoil.net"],
            stdout=subprocess.PIPE,
        ).stdout

        host = socket.gethostname()

        if host + ".client.statoil.net" in str(s):
            return True
        elif "client.statoil.net" in host and host in str(s):
            return True
    elif is_linux():
        with open("/etc/resolv.conf", "r") as f:
            if "statoil.no" in f.read():
                return True
    else:
        raise OSError(
            f"Unsupported system: {platform.system()}. Please report this as an issue."
        )
    return False


class IMSType(str, Enum):
    PIWEBAPI = "piwebapi"
    ASPENONE = "aspenone"
    PI = "pi"
    ASPEN = "aspen"
    IP21 = "ip21"

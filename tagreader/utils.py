import enum
import hashlib
import platform
import socket
import ssl
from datetime import datetime, tzinfo
from enum import Enum
from pathlib import Path
from typing import Union

import certifi
import pandas as pd
import pytz
import requests
from platformdirs import user_data_dir

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
    import subprocess


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


def add_equinor_root_certificate() -> bool:
    """
    This is a utility function for Equinor employees on Equinor managed machines.

    The function searches for the Equinor Root certificate in the
    cert store and imports it to the cacert bundle. Does nothing if not
    running on Equinor host.

    NB! This needs to be repeated after updating the cacert module.

    Returns:
        bool: True if function completes successfully
    """
    certificate = find_local_equinor_root_certificate()

    # If certificate is not found locally, we download it from the Equinor server
    if certificate == "":
        logger.debug(
            "Unable to locate Equinor Root CA certificate on this host. Downloading from Equinor server."
        )
        response = requests.get("http://pki.equinor.com/aia/ecpr.crt")

        if response.status_code != 200:
            logger.error(
                "Unable to find Equinor Root CA certificate locally and on Equinor server."
            )
            return False

        certificate = response.text.replace("\r", "")

        # Write result to user data so we can read the cert from there next time
        filepath = Path(user_data_dir("tagreader")) / "equinor_root_ca.crt"
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            filepath.write_text(certificate)
            logger.debug("Equinor Root CA certificate written to cache")
        except Exception as e:
            logger.debug(f"Failed to write Equinor Root CA certificate to cache: {e}")

    if certificate in certifi.contents():
        logger.debug("Equinor Root Certificate already exists in certifi store")
        return True

    ca_file = certifi.where()
    with open(ca_file, "ab") as f:
        f.write(bytes(certificate, "ascii"))
    logger.debug("Equinor Root Certificate added to certifi store")
    return True


def find_local_equinor_root_certificate() -> str:
    equinor_root_pem_hash = "5A206332CE73CED1D44C8A99C4C43B7CEE03DF5F"
    ca_search = "Equinor Root CA"

    if is_windows():
        logger.debug("Checking for Equinor Root CA in Windows certificate store")
        for cert in ssl.enum_certificates("CA"):  # type: ignore
            found_cert = cert[0]
            # deepcode ignore InsecureHash: <Only hashes to compare with known hash>
            if hashlib.sha1(found_cert).hexdigest().upper() == equinor_root_pem_hash:
                return ssl.DER_cert_to_PEM_cert(found_cert)

    elif is_mac():
        logger.debug("Checking for Equinor Root CA in MacOS certificate store")
        macos_ca_certs = subprocess.run(
            ["security", "find-certificate", "-a", "-c", ca_search, "-Z"],
            stdout=subprocess.PIPE,
        ).stdout

        if equinor_root_pem_hash in str(macos_ca_certs).upper():
            c = get_macos_equinor_certificates()
            for cert in c:
                # deepcode ignore InsecureHash: <Only hashes to compare with known hash>
                if hashlib.sha1(cert).hexdigest().upper() == equinor_root_pem_hash:
                    return ssl.DER_cert_to_PEM_cert(cert)

    # If the certificate is not found in the local cert store, look in the tagreader cache
    filepath = Path(user_data_dir("tagreader")) / "equinor_root_ca.crt"

    try:
        if filepath.exists():
            return filepath.read_text()
    except Exception as e:
        logger.debug(f"Failed to read Equinor Root CA certificate from cache: {e}")

    return ""


def get_macos_equinor_certificates():
    import ssl
    import tempfile

    if not is_mac():
        raise OSError("Function only works on MacOS")

    ca_search = "Equinor Root CA"

    ctx = ssl.create_default_context()
    macos_ca_certs = subprocess.run(
        ["security", "find-certificate", "-a", "-c", ca_search, "-p"],
        stdout=subprocess.PIPE,
    ).stdout
    with tempfile.NamedTemporaryFile("w+b", delete=False) as tmp_file:
        tmp_file.write(macos_ca_certs)

    ctx.load_verify_locations(tmp_file.name)

    return ctx.get_ca_certs(binary_form=True)


def is_equinor() -> bool:
    """Determines whether code is running on an Equinor host

    If Windows host:
    Finds host's domain in Windows Registry at
    HKLM\\SYSTEM\\ControlSet001\\Services\\Tcpip\\Parameters\\Domain
    or check if hostname starts with eqdev or eqpc
    If mac os host:
    Finds statoil.net as AD hostname in certificates or
    Finds statoil.net, client.statoil.net or equinor.com in dns search domains
    If Linux host:
    Checks whether statoil.no is dns search domains

    Returns:
        bool: True if Equinor
    """

    hostname = socket.gethostname()

    if is_windows():
        if hostname.lower().startswith("eqdev") or hostname.lower().startswith("eqpc"):
            return True
        with winreg.OpenKey(  # type: ignore
            winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\ControlSet001\Services\Tcpip\Parameters"  # type: ignore
        ) as key:
            domain = winreg.QueryValueEx(key, "Domain")  # type: ignore
            if "statoil" in domain[0] or "equinor" in domain[0]:
                return True
    elif is_linux() or is_mac():
        with open("/etc/resolv.conf", "r") as f:
            if any(
                domain in f.read()
                for domain in ["client.statoil.net", "statoil.net", "equinor.com"]
            ):
                return True

        if is_mac():

            def get_mac_dns_search_list():
                """
                Retrieves the DNS search list configured on macOS.
                """
                try:
                    # Execute the scutil command to get DNS configuration
                    result = subprocess.run(
                        ["scutil", "--dns"], capture_output=True, text=True, check=True
                    )
                    output = result.stdout

                    search_list = []
                    # Parse the output to find the search domains
                    for line in output.splitlines():
                        if "search domain" in line:
                            # Extract the domain from the line
                            parts = line.split(":")
                            if len(parts) > 1:
                                domain = parts[1].strip()
                                # Remove any leading/trailing quotes if present
                                domain = domain.strip('"')
                                search_list.append(domain)
                    return search_list
                except subprocess.CalledProcessError as e:
                    print(f"Error executing scutil: {e}")
                    return []
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    return []

        if any(
            domain in get_mac_dns_search_list()
            for domain in ["client.statoil.net", "statoil.net", "equinor.com"]
        ):
            return True

        s = subprocess.run(
            ["security", "find-certificate", "-a", "-c" "client.statoil.net"],
            stdout=subprocess.PIPE,
        ).stdout

        # deepcode ignore IdenticalBranches: Not an error. First test is just more precise.
        if hostname + ".client.statoil.net" in str(s):
            return True
        elif "client.statoil.net" in hostname and hostname in str(s):
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

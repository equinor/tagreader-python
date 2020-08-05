#!/usr/bin/env python
#
# This is a utility script for Equinor employees on Equinor machines.
# The script searches for the # Statoil Root certificate in the Windows
# cert store and imports it to the # cacert bundle.
#
# This only needs to be done once per virtual environment.
#

import ssl
import certifi
import hashlib

STATOIL_ROOT_PEM_HASH = "ce7bb185ab908d2fea28c7d097841d9d5bbf2c76"

print("Scanning CA certs in store ", end="")
found = False
for cert in ssl.enum_certificates("CA"):
    print(".", end="")
    der = cert[0]
    if hashlib.sha1(der).hexdigest() == STATOIL_ROOT_PEM_HASH:
        found = True
        print(" found it!")
        print("Converting certificate to PEM")
        pem = ssl.DER_cert_to_PEM_cert(cert[0])
        if pem in certifi.contents():
            print("Certificate already exists in certifi store. Nothing to do.")
            break
        print("Writing certificate to cacert store.")
        cafile = certifi.where()
        with open(cafile, "ab") as f:
            f.write(bytes(pem, "ascii"))
        print("Completed")
        break

if not found:
    print("\n\nERROR: Unable to locate Statoil Root certificate.")

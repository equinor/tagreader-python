---
sidebar_position: 3
---

# Host Certificates

## For Equinor users

***Note**: Since v2.7.0 the procedure described below will be automatically performed on Equinor hosts when importing 
the tagreader module. It should therefore no longer be necessary to perform this step manually.*

The Web APIs are queried with the requests package. Requests does not utilize the system certificate store, but instead 
relies on the certifi bundle. In order to avoid SSL verification errors, we need to either turn off SSL verification 
(optional input argument `verifySSL=False` for relevant function calls) or, strongly preferred, add the certificate to 
the certifi bundle. To do this, simply activate the virtual environment where you installed tagreader, and run the 
following snippet:

``` python
from tagreader.utils import add_statoil_root_certificate
add_statoil_root_certificate()
```

The output should inform you that the certificate was successfully added. This needs to be repeated whenever certifi is 
upgraded in your python virtual environment. It is safe to run more than once: If the function detects that the 
certificate has already been added to your current certifi installation, the certificate will not be duplicated.

## For non-Equinor users

If you run info SSL verification errors and prefer to not set `verifySSL=False` , you can try the procedure outlined 
[here](https://incognitjoe.github.io/adding-certs-to-requests.html).
from importlib.metadata import PackageNotFoundError, distribution

try:
    version = distribution("tagreader").version
except PackageNotFoundError:
    # This will happen if you run tagreader without installing it as a package. E.g. poetry install --no-root.
    version = "0.0.0"

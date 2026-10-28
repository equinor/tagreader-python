import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-15s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


logger = logging.getLogger("awt")

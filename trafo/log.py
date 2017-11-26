import logging

handler = logging.StreamHandler()


def setup_logger(name, verbosity=1):
    """
    Basic logger
    """

    formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    handler.setFormatter(formatter)

    # Set up main logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    if verbosity > 1:
        logger.setLevel(logging.DEBUG)

    logger.propagate = False
    return logger

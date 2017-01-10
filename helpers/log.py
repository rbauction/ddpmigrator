import logging


def setup_custom_logger(name):
    formatter = logging.Formatter('%(asctime)s %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

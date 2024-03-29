import logging.config


def get_logger(name):
    logging.config.fileConfig('properties/config/logging.config')
    logger = logging.getLogger(name)
    return logger


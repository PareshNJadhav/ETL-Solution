import logging

def get_logger(name='root'):


    logger = logging.getLogger(name)
    # create handler
    file_handler = logging.FileHandler('application.log')
    stream_handler = logging.StreamHandler()

    # set log format
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # set log level
    file_handler.setLevel(logging.INFO)
    stream_handler.setLevel(logging.INFO)

    # add handler to logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    return logger

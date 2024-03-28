from logger import get_logger


# logging.config.fileConfig('properties/config/logging.config')
logger = get_logger('Validate')


def get_current_date(spark):
    try:
        logger.info('Started the get_current_date function....')
        output = spark.sql('Select current_date')
        logger.info('Validating spark object with current date {} '. format(output.collect()))

    except Exception as e:
        logger.warning('An error occured in get_current_date', str(e))
        raise
    else:
        logger.info('Validation done, going forward ..')
import logging.config
import traceback

logging.config.fileConfig('properties/config/logging.config')

logger = logging.getLogger('Loggers')
logger.debug('This is test log')
try:
    1 / 0

except ArithmeticError as e:
    logger.error("The error is %s", traceback.format_exc())

from logger import get_logger
from pyspark.sql.functions import *

logger = get_logger('Extraction')


def extract_file(df, format, filepath, no_of_partitions, header_req, compressionType):
    try:
        logger.warning('Extract_file function started...')
        df.coalesce(no_of_partitions).write.mode('overwrite').format(format).save(filepath, header=header_req,
                                                                                  compression=compressionType)

    except Exception as exp:
        logger.error('An error occurred while calling extract_file() function, trace error...{}'.format(exp),
                     exc_info=True)
        raise exp
    else:
        logger.warning('extract_file function successfully executed, going forward..')

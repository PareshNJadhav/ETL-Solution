import sys
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger('create_spark')

def get_spark_object(env,appname):
    try:

        logger.warning("get_spark_object function started...")
        '''Master represents cluster on which code is running, so if env is DEV use Master cluster else if env is 
        PROD use YARN cluster'''
        if env == 'DEV':
            master = 'local'
        else:
            master = 'YARN'

        logger.warning("master is {}...".format(master))
        spark = SparkSession.builder.master(master).appName(appname).getOrCreate()
        return spark

    except Exception as exp:
        logger.error('An error occurred in the get_spark_object function, trace ..',str(exp))
        sys.exit(1)

    else:
        logger.warning('Spark object is created.. ')

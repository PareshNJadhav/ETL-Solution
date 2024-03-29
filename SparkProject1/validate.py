from logger import get_logger
from pyspark.sql.functions import *
logger = get_logger('Validate')


def get_current_date(spark):
    try:
        logger.info('Started the get_current_date function....')
        output = spark.sql('Select current_date')
        logger.warning('Validating spark object with current date {} '.format(output.collect()))

    except Exception as e:
        logger.error('An error occurred in get_current_date', str(e))
        raise
    else:
        logger.info('Validation done, going forward ..')


def print_schema(df, dfName):
    try:
        logger.info('Started print_schema function for {} '.format(dfName))
        # df.printSchema()
        sch = df.schema.fields

        for i in sch:
            logger.info(f'\t{i}')

    except Exception as e:
        logger.warning('An error occurred in print_schema {}'.format(e), exc_info=True)
    else:
        logger.info('print_schema done, going forward ..')


def check_for_nulls(df,dfName):
    try:
        """ when col is not a number or a null then alias as col name for every column, this df is created for checking
        which rows have null values """
        logger.warning('checking null values in all columns ..')
        check_null_df = df.select(
            [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
    except Exception as e:
        logger.warning('An error occurred in print_schema {}'.format(e), exc_info=True)
    else:
        logger.info('check_for_nulls done, going forward ..')

    return check_null_df





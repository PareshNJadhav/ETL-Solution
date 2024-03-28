import logging
import os
import sys
import get_env_variables as env
from create_spark import get_spark_object
from validate import get_current_date
from logger import get_logger
from ingest import load_files, display_df,df_count


def main():
    global header, file_format, file_dir, inferSchema
    try:
        spark = get_spark_object(env.env, env.app_name)
        logger.info('Spark session started....{}'.format(spark))
        logger.info('validating spark object')
        get_current_date(spark)
        for file in os.listdir(env.source_olap):
            file_dir = env.source_olap + '/' + file

            if file.endswith('.parquet'):
                file_format = 'parquet'
                inferSchema = 'NA'
                header = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = env.header
                inferSchema = env.inferSchema

        logger.info('Reading file which is of file format, {}'.format(file_format))
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logger.info("displaying Dataframe..")
        display_df(df_city, 'df_city')

        logger.info("Validating number of cities...")
        df_count(df_city, 'df_city')

        for file in os.listdir(env.source_oltp):
            file_dir = env.source_oltp + '/' + file

            if file.endswith('.parquet'):
                file_format = 'parquet'
                inferSchema = 'NA'
                header = 'NA'
            elif file.endswith('.csv'):
                file_format = 'csv'
                header = env.header
                inferSchema = env.inferSchema

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logger.info("displaying Dataframe {}".format('df_fact'))
        display_df(df_fact, 'df_fact')

        logger.info("Validating number of records in fact...")
        df_count(df_fact, 'df_fact')

    except Exception as exp:
        logger.error('An error occurred when calling main() please check trace..{}'.format(exp), exc_info=True)
        # exc_info will show traceback with lines as well
        # logger.error('AN exception occurred when calling main() please check trace' , exp, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    try:
        logger = get_logger('Driver')

        main()

        logger.info("Application done")
    except Exception as e:
        logger.error('An error occurred when calling main() please check trace..{}'.format(e), exc_info=True)
        sys.exit(1)


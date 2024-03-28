from logger import get_logger

logger = get_logger(__name__)


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning('load_files function started')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            # df = spark.read.format(file_format).option("header",header).option('inferSchema',inferSchema).load(file_dir)
            df = spark.read.format(file_format).options(header=header, inferSchema=inferSchema).load(file_dir)
    except Exception as e:
        logger.error("An erroe occured while loading files..", str(e))
        raise
    else:
        logger.info("dataframe created successfully which is of {} format".format(file_format))

    return df


def display_df(df, dfname):
    try:
        df_show = df.show()
    except Exception as e:
        raise e
    return df_show


def df_count(df, dfName):
    try:
        logger.info('here to count the records in the {}'.format(dfName))
        count = df.count()
    except Exception as e:
        raise e
    else:
        logger.info("Number of records present in the {} are {}".format(dfName, count))
    return count

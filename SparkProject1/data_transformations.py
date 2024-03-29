from udf import *
from logger import get_logger

logger = get_logger('DataTransformation')


def data_report1(df_city, df_presc):
    try:
        logger.warning('processing data in data_report1 function...')

        logger.warning('calculating total number of zipcodes in {}'.format(df_city))

        df_city_split = df_city.withColumn('zip_counts', column_split_count(df_city.zips))

        logger.warning('calculating distinct prescribers and total tx_cnt')

        df_presc_grp = df_presc.groupBy(df_presc.presc_state, df_presc.presc_city).agg(
            countDistinct('presc_id').alias('presc_counts'),
            sum('tx_cnt').alias('tx_total'))

        logger.warning(
            'do not create a report for city if no prescriber are present for that city so lets join df_city and df_presc')

        df_city_join = df_city_split.join(df_presc_grp, (df_city.state_id == df_presc_grp.presc_state) \
                                          & (df_city.city == df_presc_grp.presc_city), 'inner')

        df_final = df_city_join.select('city', 'state_name', 'county_name', 'population', 'zip_counts', 'presc_counts')
    except Exception as exp:
        logger.error('An error occurred while calling data_report1() function, trace error...{}'.format(exp),
                     exc_info=True)

    else:
        logger.warning('data_report function successfully executed, going forward..')

    return df_final

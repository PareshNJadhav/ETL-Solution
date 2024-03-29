from logger import get_logger
from pyspark.sql.functions import *
from pyspark.sql.types import *

logger = get_logger('DataProcessing')


def data_clean(df1, df2):
    try:
        logger.warning('data cleaning function started...')

        logger.warning(
            'working on OLAP dataset and selecting required columns and converting some of columns into upper case...')
        df_city_sel = df1.select(upper(col('city')).alias('city'), df1.state_id,
                                 upper(df1.state_name).alias('state_name'),
                                 upper(df1.county_name).alias('county_name'),
                                 df1.population, df1.zips)

        logger.warning(
            'working on OLTP dataset and selecting required columns and converting some of columns into upper case...')

        df_presc_sel = df2.select(df2.npi.alias('presc_id'), df2.nppes_provider_last_org_name.alias('presc_lname'),
                                  df2.nppes_provider_first_name.alias('presc_fname'),
                                  df2.nppes_provider_city.alias('presc_city'),
                                  df2.nppes_provider_state.alias('presc_state'),
                                  df2.specialty_description.alias('presc_description'),
                                  df2.drug_name, df2.total_claim_count.alias('tx_cnt'), df2.total_day_supply,
                                  df2.total_drug_cost,
                                  df2.years_of_exp)

        logger.warning('Adding new column to df_presc_sel')
        df_presc_sel = df_presc_sel.withColumn('country_name', lit('USA'))

        logger.warning('converting years_of_exp to int and replacing = ')
        df_presc_sel = df_presc_sel.withColumn('years_of_exp', regexp_replace(col('years_of_exp'), r'^=', ' '))

        df_presc_sel = df_presc_sel.withColumn('years_of_exp', col('years_of_exp').cast('int'))

        logger.warning('concat firstname and lastname..')
        df_presc_sel = df_presc_sel.withColumn('fullName', concat_ws(' ', 'presc_fname', 'presc_lname'))

        logger.warning('dropping columns presc_fname and presc_lname..')
        df_presc_sel = df_presc_sel.drop('presc_fname', 'presc_lname')

        # drop if the subset column any value is NULL then dropping that row for 2 cols which should be not null for
        # further process
        logger.warning('dropping null values for the columns ..')

        df_presc_sel = df_presc_sel.dropna(subset='presc_id')
        df_presc_sel = df_presc_sel.dropna(subset='drug_name')

        logger.warning('successfully dropped null values ..')

        # df_presc_sel = df_presc_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_presc_sel.columns])

        logger.warning('replacing the null values of tx_cnt column with avg value..')

        # Collect return rdd with list of rows, so we can use collect()[0] to take 1st row and again
        # Collect()[0][0] to get that index values

        mean_tx_cnt = df_presc_sel.select(mean('tx_cnt')).collect()[0][0]
        df_presc_sel = df_presc_sel.fillna(mean_tx_cnt, 'tx_cnt')

    except Exception as e:
        logger.error('An error occurred while calling data_clean() function, trace error...{}'.format(exp),
                     exc_info=True)

        raise
    else:
        logger.warning('data_clean() function executed, going forward..')

    return df_city_sel, df_presc_sel

from pyspark.sql import functions as F, SparkSession, DataFrame

from spark_utils import get_spark_session

def read_new_data(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.option('multiline', 'true').json(path)
    business_keys = ['id']
    business_keys_hash = F.concat_ws('||', *[F.col(c) for c in business_keys])
    original_columns_hash = F.concat_ws('||', *[F.col(c) for c in df.columns if c not in business_keys])

    # SCD2 metadata columns
    df = df.withColumn('_hashkey', F.sha2(business_keys_hash, 256))
    df = df.withColumn('_hashrecord', F.sha2(original_columns_hash, 256))
    df = df.withColumn('_valid_from', F.current_timestamp())
    df = df.withColumn('_valid_to', F.lit(None).cast('timestamp'))
    df = df.withColumn('_is_current', F.lit(True))
    return df

def write_scd2_data(spark: SparkSession, table_name: str, df: DataFrame):
    if not spark.catalog.tableExists('iceberg_lake.funcionarios_scd'):
        (df
         .writeTo(f'iceberg_lake.{table_name}').using('iceberg')
         # .tableProperty('write.spark.accept-any-schema', 'true')
         .createOrReplace())
        return

    original_columns = df.columns
    df.createOrReplaceTempView('new_data')

    # Identify deleted records
    spark.sql(f'''
    SELECT
        _hashkey
    FROM iceberg_lake.{table_name}
    WHERE _is_current = true
    AND _hashkey NOT IN (SELECT _hashkey FROM new_data)
    ''').createOrReplaceTempView('deleted_records')

    # Mark deleted records as invalid
    spark.sql(f'''
    UPDATE iceberg_lake.{table_name} target
    SET _is_current = false, _valid_to = current_timestamp()
    WHERE _hashkey IN (SELECT _hashkey FROM deleted_records)
    AND _is_current = true
    ''')

    spark.sql(f'''
    -- Get all records that were updated
    SELECT
        source._hashkey as _join_key,
        source.* 
    FROM new_data source
    JOIN iceberg_lake.{table_name} target 
        ON source._hashkey = target._hashkey AND target._is_current = true AND source._hashrecord <> target._hashrecord
    UNION ALL
    -- Duplicate all records that were updated, to force the new version insert
    SELECT 
        NULL as _join_key,
        source.*
    FROM new_data source
    JOIN iceberg_lake.{table_name} target 
        ON source._hashkey = target._hashkey AND target._is_current = true AND source._hashrecord <> target._hashrecord
    UNION ALL
    -- Get all records that were new
    SELECT 
        NULL as _join_key,
        source.*
    FROM new_data source
    LEFT JOIN (
        SELECT DISTINCT _hashkey FROM iceberg_lake.{table_name} WHERE _is_current = true
    ) target
        ON source._hashkey = target._hashkey
    WHERE target._hashkey IS NULL
    ''').createOrReplaceTempView('prepared_source')

    # List all columns except _join_key to the INSERT
    insert_columns = ", ".join([f"source.{col}" for col in original_columns])

    spark.sql(f'''
    MERGE INTO iceberg_lake.{table_name} AS target
    USING prepared_source AS source
    ON target._hashkey = source._join_key AND target._is_current = true
    -- Close existing current records for updated entities
    WHEN MATCHED THEN UPDATE SET
        target._valid_to = source._valid_from,
        target._is_current = false
    -- Insert new records
    WHEN NOT MATCHED THEN INSERT ({", ".join(original_columns)}) VALUES ({insert_columns})
    ''')
    return


spark_session = get_spark_session()
spark_session.sql('create database if not exists iceberg_lake')

# df_new = read_new_data(spark_session, 's3a://victorouttes-landing/raw/scd_full/funcionarios_1.json')
# df_new = read_new_data(spark_session, 's3a://victorouttes-landing/raw/scd_full/funcionarios_2.json')
df_new = read_new_data(spark_session, 's3a://victorouttes-landing/raw/scd_full/funcionarios_3.json')
write_scd2_data(spark_session, 'funcionarios_scd', df_new)

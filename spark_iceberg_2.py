from pyspark.sql import functions as F

from spark_utils import get_spark_session

spark = get_spark_session()

business_key = 'id'

# df = spark.read.option('multiline', 'true').json('s3a://victorouttes-landing/raw/eventos.json')
# df = df.withColumn('_hashkey', F.sha2(F.concat_ws('||', F.col(business_key)), 256))
# df = df.withColumn('_valid_from', F.current_timestamp())
# df = df.withColumn('_valid_to', F.lit(None).cast('timestamp'))
# df = df.withColumn('_is_current', F.lit(True))
#
# spark.sql('create database if not exists iceberg_lake')
#
# df.writeTo('iceberg_lake.eventos_scd').using('iceberg').tableProperty('write.spark.accept-any-schema', 'true').createOrReplace()

# lendo dados do iceberg
df_read = spark.read.table('iceberg_lake.eventos_scd')
df_read.show(truncate=False)

# lendo dados novos, com schema diferente
df_2 = spark.read.option('multiline', 'true').json('s3a://victorouttes-landing/raw/eventos_3.json')
df_2 = df_2.withColumn('_hashkey', F.sha2(F.concat_ws('||', F.col(business_key)), 256))
df_2 = df_2.withColumn('_valid_from', F.current_timestamp())
df_2 = df_2.withColumn('_valid_to', F.lit(None).cast('timestamp'))
df_2 = df_2.withColumn('_is_current', F.lit(True))
df_2.createOrReplaceTempView('new_data')

# Verificar se há diferença de schema
target_columns = [col for col in spark.read.table('iceberg_lake.eventos_scd').columns if not col.startswith('_')]  # Excluindo colunas de controle
new_columns = [col for col in df_2.columns if not col.startswith('_')]  # Excluindo colunas de controle
schema_changed = set(target_columns) != set(new_columns)

# Se o schema mudou, tratamos todos os registros com mesmo ID como atualizados
if schema_changed:
    # Para todos os registros com IDs em comum, considera como modificados devido à mudança de schema
    spark.sql(f"""
    SELECT 
        new_data._hashkey as hashkey 
    FROM new_data
    JOIN iceberg_lake.eventos_scd target 
    WHERE target._hashkey = new_data._hashkey and target._is_current = true
    """).createOrReplaceTempView('updated_data')
else:
    # Se não houve mudança de schema, verifica alterações coluna a coluna
    original_columns = [col for col in df_2.columns if not col.startswith('_')]
    check_columns_changes = ' or '.join([f'target.{col}<>new_data.{col}' for col in original_columns])

    spark.sql(f"""
        SELECT 
            new_data._hashkey as hashkey 
        FROM new_data
        JOIN iceberg_lake.eventos_scd target 
        WHERE target._hashkey = new_data._hashkey and target._is_current = true and ({check_columns_changes})
        """).createOrReplaceTempView('updated_data')

spark.sql(f"""
UPDATE iceberg_lake.eventos_scd target
SET _is_current = false, _valid_to = current_timestamp()
WHERE (_hashkey) IN (SELECT hashkey FROM updated_data)    
""")

df_to_be_inserted = spark.sql(f"""
SELECT 
    new_data.* 
FROM new_data
WHERE new_data._hashkey not in (SELECT _hashkey FROM iceberg_lake.eventos_scd WHERE _is_current = true)
UNION
SELECT 
    new_data.* 
FROM new_data
WHERE new_data._hashkey IN (SELECT hashkey FROM updated_data)
""")

df_to_be_inserted.writeTo('iceberg_lake.eventos_scd').using('iceberg').option('mergeSchema', 'true').append()

# lendo dados do iceberg
df_read = spark.read.table('iceberg_lake.eventos_scd')
df_read.show(truncate=False)

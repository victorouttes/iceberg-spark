from spark_utils import get_spark_session

spark = get_spark_session()
df = spark.read.option('multiline', 'true').json('s3a://victorouttes-landing/raw/eventos.json')

spark.sql('create database if not exists iceberg_lake')

df.writeTo('iceberg_lake.eventos').using('iceberg').tableProperty('write.spark.accept-any-schema', 'true').createOrReplace()

# lendo dados do iceberg
df_read = spark.read.table('iceberg_lake.eventos')
df_read.show(truncate=False)

# lendo dados novos, com schema diferente
df_2 = spark.read.option('multiline', 'true').json('s3a://victorouttes-landing/raw/eventos_2.json')

df_2.writeTo('iceberg_lake.eventos').using('iceberg').option('mergeSchema', 'true').append()

# lendo dados do iceberg
df_read = spark.read.table('iceberg_lake.eventos')
df_read.show(truncate=False)

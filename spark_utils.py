import os

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession

load_dotenv()

def get_spark_session():
    builder = SparkSession.builder.appName('Spark Iceberg Demo')
    builder = (builder
               .config('spark.sql.shuffle.partitions', '200')
               .config('spark.default.parallelism', '200')
               .config('spark.memory.offHeap.enabled', 'true')
               .config('spark.memory.offHeap.size', '2g'))

    # jars
    project_root = os.path.join(os.getcwd(), 'jars')
    jar_files = [
        'aws-java-sdk-bundle-1.12.696.jar',
        'hadoop-aws-3.3.4.jar',
        'iceberg-aws-bundle-1.9.2.jar',
        'iceberg-spark-runtime-3.5_2.12-1.9.2.jar',
    ]
    jar_paths = [os.path.join(project_root, jar_file) for jar_file in jar_files]
    jars_string = ','.join(jar_paths)
    builder = builder.config('spark.jars', jars_string)

    # configurações para S3
    builder = (builder
               .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
               .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')
               .config('spark.hadoop.fs.s3a.region', 'us-east-1'))

    # configurações para iceberg com glue
    builder = (builder
               .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
               .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
               .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
               .config('spark.sql.catalog.glue_catalog.warehouse', 's3a://victorouttes-landing/dw')
               .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
               .config('spark.sql.catalog.glue_catalog.glue.id-length', '255')
               .config('spark.sql.catalog.glue_catalog.table-default.format-version', '2')
               .config('spark.sql.catalog.glue_catalog.table-default.write.parquet.compression-codec', 'snappy')
               .config('spark.sql.catalog.glue_catalog.table-default.write.distribution-mode', 'hash')
               .config('spark.sql.iceberg.check-ordering', 'false')
               .config('spark.sql.catalog.glue_catalog.aws.region', 'us-east-1')
               .config('spark.sql.catalog.glue_catalog.database', 'iceberg_lake')
               .config('spark.sql.defaultCatalog', 'glue_catalog')
               .config('spark.sql.iceberg.handle-timestamp-without-timezone', 'true'))

    logger.info('Building Spark Session...')
    spark = builder.getOrCreate()
    logger.info(f'Spark Session Built! Version: {spark.version}')
    return spark



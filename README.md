# iceberg-spark
Projeto exemplo para rodar o Spark com Iceberg. O mais interessante é a implementação de um SCD tipo 2 com
Iceberg.

Você deve baixar as seguintes versões de *.jar e colocar na pasta `jars` do projeto:
- aws-java-sdk-bundle-1.12.696.jar
- hadoop-aws-3.3.4.jar
- iceberg-aws-bundle-1.9.2.jar
- iceberg-spark-runtime-3.5_2.12-1.9.2.jar

Instalar as dependências com:
```bash
pip install -r requirements.txt
```

Copiar o conteudo da pasta `s3data` para o bucket S3 escolhido. Lembrar de atualizar os scripts com o nome do bucket.

Criar um arquivo `.env` com o conteudo (o SESSION_TOKEN é opcional):
```
AWS_ACCESS_KEY_ID=""
AWS_SECRET_ACCESS_KEY=""
AWS_SESSION_TOKEN=""
AWS_REGION="us-east-1"
```

Por fim, executar os scripts desejados:
```bash
python spark_iceberg_1.py
python spark_iceberg_2.py
python spark_iceberg_scd2_full.py
# ...
```
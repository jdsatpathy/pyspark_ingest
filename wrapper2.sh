SPARK_MAJOR_VERSION=2
spark-submit \
--master yarn \
--deploy-mode cluster \
--files application.properties \
ingest2.py prod

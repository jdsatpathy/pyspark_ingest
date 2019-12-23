from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import configparser as cp
import sys

try:
  def main():
    #conf = SparkConf.setMaster("yarn").setAppName("Demo App")
    env_prop = cp.ConfigParser()
    env_prop.read("application.properties")
    env = sys.argv[1]
    input_path = env_prop.get(env, "input.path")
    src_schema = read_schema(spark)
    conf = spark_conf_creator(env_prop, env)
    spark = spark_session_creator(env_prop, env, conf)
    crime = spark.read.format("csv").load(input_path).schema(src_schema)
    return None  


  def read_schema(spark, env_prop):
    schema = {"":""}
    spark.read.table("jd_itver.crime_schema").map(lambda r : schema.update({r.col_name : r.col_type}))
    return schema


  def read_data(spark, env_prop, env):
    crime = spark.read.format(env_prop.get(env, "file.format")).load(input_path).schema(src_schema)
    return crime


  def spark_conf_creator(env_prop, env):
    conf = SparkConf.setAppName("File Ingestion")
    return conf

  def spark_session_creator(env_prop, env, conf):
    spark = SparkSession.builder.conf(conf=conf).getOrCreate() 
    return spark

  def cleanse_data():
    pass

  def load_data():
    df.insertInto("jd_itver.crime_table")

  if (__name__ == '__main__'):
    main()

except Exception as e:
  print(e.message)

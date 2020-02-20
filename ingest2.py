from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import configparser as cp
import sys
import logging
import os

# import my_logger
# sys.path.append(".")

def my_logger():
    logger = logging.getLogger(__name__)
    # Adding file logger
    f_handler = logging.FileHandler(__file__ + ".log")
    f_formater = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s : %(message)s")
    f_handler.setFormatter(f_formater)
    f_handler.setLevel(logging.ERROR)
    f_handler.setLevel(logging.WARNING)
    f_handler.setLevel(logging.INFO)
    logger.addHandler(f_handler)

    # Adding console logger
    c_handler = logging.StreamHandler()
    c_formater = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s : %(message)s")

    c_handler.setFormatter(c_formater)
    c_handler.setLevel(logging.ERROR)
    c_handler.setLevel(logging.WARNING)
    c_handler.setLevel(logging.INFO)
    logger.addHandler(c_handler)
    return logger

try: 
    def main(logger):
        # Main program starts

        # conf = SparkConf.setMaster("yarn").setAppName("Demo App")
        env_prop = cp.ConfigParser()
        #logger.warning(str(os.path.dirname(__file__)))
        env_prop.read(os.path.dirname("") + "/application.properties")
        #env_prop.read("src/main/resources/application.properties")
        env = sys.argv[1]
        # logger.warn(env_prop.sections())
        input_path = env_prop.get(env, "input.path")
        # logger.warn(input_path)
        # logger.error(input_path)
        # src_schema = read_schema(spark)
        conf = spark_conf_creator(env_prop, env)

        spark = spark_session_creator(env_prop, env, conf)
        # sc = new SparkContext(conf=conf)
        # crime = spark.read.format("csv").load(input_path).schema(src_schema)
        # logger.warning("Message is : %s", input_path, exc_info=True)
        logger.warning(input_path)  # exc_info=None, extra=None, stack_info=False
        logger.warning(str(os.path.dirname(__file__)))


    def read_schema(spark, env_prop):
        schema = {"": ""}
        spark.read.table("jd_itver.crime_schema").map(lambda r: schema.update({r.col_name: r.col_type}))
        return schema


    def read_data(spark, env_prop, env):
        crime = spark.read.format(env_prop.get(env, "file.format")).load(input_path).schema(src_schema)
        return crime


    def spark_conf_creator(env_prop, env):
        conf = SparkConf().setAppName("File Ingestion").setMaster("yarn")
        return conf


    def spark_session_creator(env_prop, env, conf):
        spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        #spark = SparkSession.builder.master("yarn").appName("xyz").getOrCreate()
        return spark


    def cleanse_data():
        pass


    def load_data(df):
        df.insertInto("jd_itver.crime_table")

    """
    if (__name__ == '__main__'):
        logger = my_logger()
        main(logger)
    """    
except Exception as e:
    #sys.exit(14)
    logger.error(e.message, exc_info=1)


if (__name__ == '__main__'):
    env_prop = cp.ConfigParser()
    env_prop.read(os.path.dirname(__file__) + "application.properties")
    env = sys.argv[1]    
    conf = spark_conf_creator(env_prop, env)
    spark = spark_session_creator(env_prop, env, conf)

    #env_prop = cp.ConfigParser()
    logger = my_logger() # This is my logger creation
    logger.warning(str(os.path.dirname(__file__)) + "application.properties")
    #env_prop.read(os.path.dirname("") + "/application.properties")
    #env = sys.argv[1]
    # logger.warn(env_prop.sections())
    input_path = env_prop.get(env, "input.path")
    #conf = spark_conf_creator(env_prop, env)
    #spark = spark_session_creator(env_prop, env, conf)
    logger.warning(input_path)  # exc_info=None, extra=None, stack_info=False
    logger.warning(str(os.path.dirname(__file__)))    
    #conf = SparkConf().setAppName("File Ingestion").setMaster("yarn") 
    #spark = SparkSession.builder.master("yarn").appName("xyz").getOrCreate()
    #logger = my_logger()
    #logger.warning(str(os.path.dirname(__file__)) + "application.properties")
    #main(logger)

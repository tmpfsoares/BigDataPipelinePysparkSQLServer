from pyspark.sql import SparkSession
from pyspark import SparkConf


class SpkSession:
    def __init__(self, **spark_args):
        self.app_name = spark_args.get('app_name', "Spark Application")
        self.master = spark_args.get('master', "yarn") 
        self.executors = spark_args.get('executors', "3")
        self.driver_memory = spark_args.get('driver_memory', "512M")
        self.executor_memory = spark_args.get('executor_memory', "512M") 
        self.executor_cores = spark_args.get('executor_cores', "1")

    def create_session(self):
        """Create a Spark Session"""

        conf = SparkConf().setAppName(self.app_name).setMaster(self.master)\
            .set('spark.driver.memory', self.driver_memory).set('spark.executor.memory', self.executor_memory)\
            .set("spark.executor.instances", self.executors).set("spark.executor.cores", self.executor_cores)

        spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        return spark_session


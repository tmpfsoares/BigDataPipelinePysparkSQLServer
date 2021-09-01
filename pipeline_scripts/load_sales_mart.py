from datetime import datetime
import logging
from pyspark.sql.functions import when, col, lit
import pandas as pd


class LoadSalesMart:
    """This class provides the methods for our second pipeline that transforms the data previously loaded to 
    Hadoop/Hive to a table called sales_history and loads it into the tables of a data mart hosted on SQL Server."""
    def __init__(self, my_conn, my_engine, my_spark, **launch_args):
        self.conn = my_conn
        self.eng = my_engine
        self.spk = my_spark
        self.log_level=launch_args.get('log_level','WARN')
        logging.basicConfig(level=self.log_level, format="%(asctime)s %(levelname)s %(threadName)s: %(message)s")

    def load_sales_mart_staging(self):
        """This method performs a few transformations to the data and loads it to two staging tables on data mart
        hosted on SQL Server."""

        logging.info('Initiating pipeline for the data mart tables.')
        try:
            spark = self.spk

            # Acquiring 1st dataset
            df = spark.sql("SELECT country, gender, COUNT(DISTINCT(client_id)) client_count "
            "FROM sales_history "
            "WHERE paid > 0 "
            "GROUP BY country, gender")

            # Applying further transformations
            now = datetime.now()
            df = df.withColumn("gender", when(col("gender")=="M","Male")
            .when(col("gender")=="F","Female").otherwise("Other"))\
            .withColumn("refresh_date", lit(now))

            # Converting it to a pandas dataframe
            df = df.select("*").toPandas()
            logging.info('First spark dataset acquired successfully.')

        except Exception as Exc:
            logging.error('Pipeline failed while transforming sales_history 1st dataset with Pyspark with the message: '
                          + str(Exc))
            raise Exc

        mart_conn = self.conn
        cursor = mart_conn.cursor()

        # Loading 1st staging table
        logging.debug('Truncating data mart staging tables.')
        cursor.execute("TRUNCATE TABLE dbo.Sales_History_1_Staging "
        "TRUNCATE TABLE dbo.Sales_History_2_Staging ")
        mart_conn.commit()

        df.to_sql("Sales_History_1_Staging", self.eng, if_exists = "append", index = None)
        logging.info('First staging table loaded successfully.')

        try:
            # Acquiring 2nd dataset
            df = spark.sql("SELECT country, product, size, color, "
            "COUNT(id) sales_count, SUM(paid) paid_amount "
            "FROM sales_history "
            "WHERE paid > 0 "
            "GROUP BY country, product, size, color")

            now = datetime.now()
            df = df.withColumn("refresh_date", lit(now))

            # Converting it to a pandas dataframe
            df = df.select("*").toPandas()
            logging.info('Second spark dataset acquired successfully.')
        except Exception as Exc:
            logging.error('Pipeline failed while transforming sales_history 2nd dataset with Pyspark with the message: '
                          + str(Exc))
            raise Exc

        # Loading 2nd staging table
        df.to_sql("Sales_History_2_Staging", self.eng, if_exists = "append", index = None)
        logging.info('Second staging table loaded successfully.')

        cursor.close()
        self.eng.dispose()

    def load_sales_mart(self):
        """This method finishes the entire process by copying the data from the staging tables to
        the final tables from which Power BI will ingest the data. The transaction will ensure that
        there's no dirty reads while truncating the data from the previous run."""
        mart_conn = self.conn
        cursor = mart_conn.cursor()

        try:
            cursor.execute("BEGIN TRANSACTION "
                           "TRUNCATE TABLE dbo.Sales_History_1 "
                           "INSERT INTO dbo.Sales_History_1 "
                           "SELECT * FROM dbo.Sales_History_1_Staging "
                           "COMMIT "
                           "BEGIN TRANSACTION "
                           "TRUNCATE TABLE dbo.Sales_History_2 "
                           "INSERT INTO dbo.Sales_History_2 "
                           "SELECT * FROM dbo.Sales_History_2_Staging "
                           "COMMIT")
            mart_conn.commit()
            logging.info('Data transferred from staging tables to visualization tables successfully.')
        except Exception as Exc:
            logging.error('Execution of SQL Server transaction failed with message: ' + Exc)
            raise Exc
        finally:
            cursor.close()






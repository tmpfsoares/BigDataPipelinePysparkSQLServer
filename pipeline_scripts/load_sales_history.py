import logging
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.types import *


class LoadSalesHistory:
    """This class provides the methods for our first pipeline for the sales_history table to be hosted on hive
     that allow to ingest and load data from a fictional transactional database called Production into a Big
     Data table called sales_history hosted on Apache Hadoop/Hive"""
    def __init__(self, my_conn, my_engine, my_spark, **launch_args):
        self.conn = my_conn
        self.eng = my_engine
        self.spk = my_spark
        self.chk_size = launch_args.get('chunksize',10000)
        self.log_level=launch_args.get('log_level','WARN')
        logging.basicConfig(level=self.log_level, format="%(asctime)s %(levelname)s %(threadName)s: %(message)s")

    def get_sales_lineage(self):
        """This method gets the starting parameters of the pipeline from the Sales_History_Lineage table."""

        prod_conn = self.conn
        cursor = prod_conn.cursor()
        # Getting current pipeline ID
        cursor.execute("SELECT ISNULL(MAX(ID),0) + 1 AS ID FROM Sales_History_Lineage "
                                "WHERE Pipeline_Status = 'SUCCESSFUL' AND Validation_Status = 'SUCCESSFUL'")
        pipeline_id = cursor.fetchone()[0]

        # Deleting previous unfinished, erroneous pipeline execution records
        cursor.execute("DELETE FROM Sales_History_Lineage WHERE ID >= %d;", pipeline_id)        
        prod_conn.commit()

        # Calculating current cut-off date
        now = datetime.now()
        current_cutoff = datetime(now.year, now.month, now.day, now.hour, now.minute)
        current_cutoff = current_cutoff-timedelta(minutes=5)

        # Calculating previous cut-off date
        cursor.execute("SELECT Current_Cutoff FROM Sales_History_Lineage WHERE ID = (%d - 1);", pipeline_id)
        prev_cutoff = cursor.fetchone()
        if prev_cutoff == None:
            previous_cutoff = prev_cutoff
        else:
            previous_cutoff = prev_cutoff[0]

        cursor.close()

        return prod_conn, pipeline_id, previous_cutoff, current_cutoff


    def load_sales_history(self):
        """This method loads the data lake table sales_history in Hive"""

        logging.info('The pipeline process has been initiated.')

        prod_conn, pipeline_id, previous_cutoff, current_cutoff = self.get_sales_lineage()
        cursor = prod_conn.cursor()
        now = datetime.now()

        logging.info('Pipeline parameters were acquired successfully.')

        # Initiating pipeline
        cursor.execute("INSERT INTO dbo.Sales_History_Lineage(ID, Exec_Start, Previous_Cutoff, Current_Cutoff, Pipeline_Status, Validation_Status) "
        "VALUES(%d, %s, %s, %s, 'RUNNING', 'NOT STARTED')", (pipeline_id, now, previous_cutoff, current_cutoff))
        prod_conn.commit()

        logging.info('The pipeline record has been created. Pipeline ID: %s. Previous Cut-Off Date: %s. Current Cut-Off Date: %s.'
        %(pipeline_id, previous_cutoff, current_cutoff) )

        # Getting Year_Month partitions that changed since last run
        if previous_cutoff == None:
            logging.warn('A previous cut-off date does not exist. A full load up to the current cut-off will be performed.')
            cursor.execute("SELECT Year_Month FROM dbo.Sales "
                "LEFT OUTER JOIN dbo.Clients ON Sales.Client_ID = Clients.ID "
                "LEFT OUTER JOIN dbo.Products ON Sales.Product_ID = Products.ID "
                "WHERE Sale_Date < %s "
                "GROUP BY Year_Month", current_cutoff)
        else:
            logging.info('Only partitions that were modified between previous and current cut-off dates will be re-built.')
            cursor.execute("SELECT Year_Month FROM dbo.Sales "
                "LEFT OUTER JOIN dbo.Clients ON Sales.Client_ID = Clients.ID "
                "LEFT OUTER JOIN dbo.Products ON Sales.Product_ID = Products.ID "
                "WHERE Sale_Date >= %s AND Sale_Date < %s "
                "GROUP BY Year_Month "                
                "UNION "                    
                "SELECT Year_Month FROM dbo.Sales "
                "LEFT OUTER JOIN dbo.Clients ON Sales.Client_ID = Clients.ID "
                "LEFT OUTER JOIN dbo.Products ON Sales.Product_ID = Products.ID "
                "WHERE Updated_Date >= %s AND Updated_Date < %s "
                "GROUP BY Year_Month "                
                "UNION "                    
                "SELECT Year_Month FROM dbo.Removed "
                "WHERE Deleted_Date >= %s AND Deleted_Date < %s "
                "GROUP BY Year_Month", (previous_cutoff, current_cutoff, previous_cutoff, current_cutoff, previous_cutoff, current_cutoff))            
        partitions = [a for (a,) in cursor.fetchall()]
        partitions.sort()
        logging.info('Partitions to be re-built: %s.' %(partitions))

        try:    
            spark = self.spk
            spark.sql("CREATE TABLE IF NOT EXISTS sales_history(id INT, sale_date TIMESTAMP, paid DECIMAL(18,2), "
                    "client_id INT, gender STRING, product_id INT, product STRING, size STRING, color STRING, "
                    "updated_date TIMESTAMP) PARTITIONED BY (year_month INT, country STRING) STORED AS PARQUET;")
            logging.info('Table sales_history created successfully in Hive if not existent.')
        except Exception as Exc:
            logging.error('Pipeline failed when creating sales_history on Hive through Pyspark with message: ' + str(Exc))
            raise Exc

        # Getting data for each of the partitions that changed and for records that were created before the cut-off
        for partition in partitions:
            logging.debug('Current partition being processed: %s.' %(partition))
            query = "SELECT Sales.ID, Sale_Date, Year_Month, Paid, Client_ID, Clients.Gender, Clients.Country, Product_ID, " \
            "Products.Product, Products.Size, Products.Color, Updated_Date FROM dbo.Sales " \
            "LEFT OUTER JOIN dbo.Clients ON Sales.Client_ID = Clients.ID " \
            "LEFT OUTER JOIN dbo.Products ON Sales.Product_ID = Products.ID " \
            "WHERE Year_Month = {0} AND Sale_Date < '{1}' ".format(partition, current_cutoff)

            df_chunks = pd.read_sql_query(query, con=self.eng, chunksize=self.chk_size,
                                        parse_dates=['Sale_Date', 'Updated_Date'])
            i = 1

            try:
                logging.info('Processing partition chunks from Pandas to Spark.')
                for chunk in df_chunks:
                    # Converting Updated_Date to string as spark is unable to process the NaT fields coming from Pandas dataframe,
                    # this is converted back into datetime downstream
                    chunk['Updated_Date']=chunk['Updated_Date'].astype('str')
                    if i == 1:
                        df_spk = spark.createDataFrame(chunk)
                        i += 1
                        logging.debug('Processing 1st chunk.')
                    else:
                        df_spk2 = spark.createDataFrame(chunk)
                        df_spk = df_spk.union(df_spk2)
                        logging.debug('Processing a subsequent chunk.')
            except Exception as Exc:
                logging.error('Pipeline failed while data chunks were being processed by Pandas and Pyspark with message: ' + str(Exc))
                raise Exc        

            def replace_nat(rec):
                """Function that will be mapped to the each of the records in the rdd, that will replace NaT values as
                well as convert datetime string values into datetime objects"""
                Up_Date = rec.Updated_Date
                if Up_Date == 'NaT':
                    Up_Date = None
                else:
                    Up_Date = datetime.strptime(Up_Date, "%Y-%m-%d %H:%M:%S")

                return (rec.ID, rec.Sale_Date, rec.Paid, rec.Client_ID, rec.Gender, rec.Product_ID,
                rec.Product, rec.Size, rec.Color, Up_Date, rec.Year_Month, rec.Country)
            
            logging.debug('Applying rdd transformation to dataframe.')
            rdd_spk = df_spk.rdd.map(lambda x: replace_nat(x))

            # Building the schema for the final spark dataframe
            schema = StructType([StructField("id", IntegerType(),False), StructField("sale_date", TimestampType(),False),
                            StructField("paid", FloatType(),False), StructField("client_id", IntegerType(),False),
                            StructField("gender", StringType(),False), StructField("product_id", IntegerType(),False), 
                            StructField("product", StringType(),False), StructField("size", StringType(), False), 
                            StructField("color", StringType(), False), StructField("updated_date", TimestampType(),True), 
                            StructField("year_month", IntegerType(),False), StructField("country", StringType(),False)])

            # Creating dataframe once again after changes
            logging.debug('Recreating spark dataframe and creating the temporary view for loading the data.')
            df_spk = spark.createDataFrame(rdd_spk, schema)
            df_spk.createOrReplaceTempView("sales_staging")

            # Dropping partition, one by one on the cycle, this applies only to the ones where changes were detected
            # between cut-off times
            logging.debug('Dropping partition %s on Hive and re-creating it with the new data.' % (partition))
            try:
                spark.sql("ALTER TABLE sales_history DROP IF EXISTS PARTITION(year_month = {0});".format(partition))
                spark.sql("INSERT INTO sales_history SELECT * FROM sales_staging;")
                logging.info('Partition %s loaded to Hive successfully.' % (partition))
            except Exception as Exc:
                logging.error('Pipeline failed when dropping a partition on Hive and inserting new data with the message: ' + str(Exc))
                raise Exc

        # Updating pipeline status
        now = datetime.now()
        cursor.execute("UPDATE dbo.Sales_History_Lineage SET Exec_Finish = %s, Pipeline_Status = 'SUCCESSFUL' "
        "WHERE ID = %d", (now, pipeline_id))
        prod_conn.commit()
        cursor.close()
        logging.info('All data was loaded to Hive successfully.')

        return prod_conn, spark, pipeline_id, current_cutoff, partitions

    def launch_and_validate(self):
        """This method calls the load_sales_history method and proceeds to performing a validation step for
        the pipeline."""

        prod_conn, spark, pipeline_id, current_cutoff, partitions = self.load_sales_history()

        logging.info('Starting pipeline validation.')

        # Updating pipeline execution data
        now = datetime.now()
        cursor = prod_conn.cursor()
        cursor.execute("UPDATE dbo.Sales_History_Lineage SET Validation_Start = %s, Validation_Status = 'RUNNING' "
        "WHERE ID = %d", (now, pipeline_id))
        prod_conn.commit()

        # Adding if condition for when there are no changes, else all necessary operations are executed
        if partitions == []:
            val_status = 'SUCCESSFUL'
            logging.info('No partitions to validate. Validation complete.')
        else:
            partition_str = [str(x) for x in partitions]
            partition_string = ', '.join(partition_str)

            # Getting validation data from RDBMS
            query = "SELECT ISNULL(COUNT(ID),0) sale_count, ISNULL(SUM(Paid),0) paid_sum FROM dbo.Sales "\
                    "WHERE Sale_Date < '" + current_cutoff.strftime("%Y-%m-%d %H:%M:%S") + "' AND Year_Month IN (" + partition_string + ")"
            try:
                cursor.execute(query)
                result_set1 = cursor.fetchone()
                sale_count = result_set1[0]      
                paid_sum = result_set1[1]
            except Exception as Exc:
                logging.error('Validation failed when calculation validation data on the RDBMS side with the message: ' + str(Exc))
                raise Exc

            # Getting validation data from Hive
            query = "SELECT IFNULL(COUNT(id),0) sale_count, IFNULL(SUM(paid),0) paid_sum FROM sales_history "\
                    "WHERE year_month IN (" + partition_string + ")"

            try:
                result = spark.sql(query)
                result_set2 = result.collect()
                sale_count_hive = result_set2[0][0]
                paid_sum_hive = result_set2[0][1]         
            except Exception as Exc:
                logging.error('Validation failed when calculation validation data from Hive side with the message: ' + str(Exc))
                raise Exc

            logging.info('Sales count on RDBMS: %s.\nSales count on Hive: %s.\n\nPaid Sum in RDBMS: %s.\nPaid Sum in Hive: %s.'
            % (sale_count, sale_count_hive, paid_sum, paid_sum_hive))

            # Checking if the data loaded to hive matches the data from the production database 
            if sale_count == sale_count_hive and paid_sum == paid_sum_hive:
                val_status = 'SUCCESSFUL'
                logging.info('Validation was successful.')
            else:
                val_status = 'FAILED'
                logging.error('Validation failed.')

        now = datetime.now()
        cursor.execute("UPDATE dbo.Sales_History_Lineage SET Validation_Finish = %s, Validation_Status = %s "
        "WHERE ID = %d", (now, val_status, pipeline_id))
        prod_conn.commit()
        cursor.close()


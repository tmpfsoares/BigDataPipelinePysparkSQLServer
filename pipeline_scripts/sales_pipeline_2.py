import sql_conn
import spark_session
import load_sales_mart
import time


# Timing execution
start_time = time.time()

# Create SQL Server engine and connection
conn_sales_mart = sql_conn.GenConn(db_name="Production_Mart")
conn_mart = conn_sales_mart.create_conn()
eng_mart = conn_sales_mart.create_eng()

# Create Spark Session
my_spk = spark_session.SpkSession(app_name="Load Sales Mart").create_session()

# Executing the pipeline
lsm = load_sales_mart.LoadSalesMart(conn_mart, eng_mart, my_spk, log_level='INFO')
lsm.load_sales_mart_staging()
lsm.load_sales_mart()

# Timing execution
print("Program took %s seconds" % (time.time()-start_time))

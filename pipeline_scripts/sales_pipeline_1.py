import sql_conn
import spark_session
import load_sales_history as lsh
import time


# Timing execution
start_time = time.time()

# Defining pipeline chunksize for pandas 
my_chunk = 20000

# Create SQL Server engine and connection
conn_production = sql_conn.GenConn()
eng_prod = conn_production.create_eng()
conn_prod = conn_production.create_conn()

# Create Spark Session
my_spk = spark_session.SpkSession(app_name = "Load Sales History").create_session()

# Executing the pipeline
lsh.LoadSalesHistory(conn_prod, eng_prod, my_spk, chunksize=my_chunk, log_level='INFO').launch_and_validate()

# Timing execution
print("Program took %s seconds" % (time.time()-start_time))

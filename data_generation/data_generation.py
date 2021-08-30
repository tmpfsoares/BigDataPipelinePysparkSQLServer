import sys
import numpy as np
import pymssql
import configparser as cr
from datetime import datetime, timedelta


def sql_connect():
    """We start by creating a connection to SQL Server, credentials are not changed here they are read from the
    configuration file cred.conf present in the folder 'config'"""
    parser = cr.ConfigParser()
    parser.read('config/cred.conf')
    user = parser.get("SQL_SERVER","user")
    pw = parser.get("SQL_SERVER","password")
    host = parser.get("SQL_SERVER","host")
    port = parser.get("SQL_SERVER","port")
    db_name = parser.get("SQL_SERVER","database")

    # Create a connection
    my_conn = pymssql.connect(server=host, port=port, user=user, password=pw, database=db_name)
    return my_conn


def generate_data(no_records,run_date):
    """On this portion of the code the data gets generated and inserted into the production database, this also
    includes random removal of records and random updates of existing records, the goal here is to mimic common
    operations that would occurs on a real-life scenario on a transactional database"""
    conn = sql_connect()
    cursor = conn.cursor()

    countries = ['Austria','Sweden','Spain','Slovenia','Slovakia','Romania','Portugal','Poland',
                 'Netherlands','Malta','Luxembourg','Lithuania','Latvia', 'Italy','Ireland','Hungary',
                 'Greece','Germany','France','Finland','Estonia','Denmark','Czech Republic','Cyprus',
                 'Croatia','Bulgaria','Belgium']

    genders = ['M','F']

    year_month = str(run_date.year) + ('0' + str(run_date.month))[-2:]
    print(year_month)


    # Inserting specified no. of records
    i = 1
    while i < no_records + 1:
        # Choosing whether it's a new client sale or a sale from an existing client, we're giving an higher probability
        # that it will an existing one (0.65)
        sale_type = np.random.choice(2,1,p=[0.35,0.65])
        if sale_type[0] == 0:
            # New customer sale

            # Get possible sale products
            cursor.execute("SELECT ID, Price FROM dbo.Products")
            prod_IDs = {x: y for (x, y) in cursor.fetchall()}

            # Randomly getting a product ID for the sale we're generating
            sale_prod = np.random.choice(list(prod_IDs.keys()),1)[0]
            # Getting price of the product sold, sometimes (p=0.01) the product will cost 0€ as it will be considered as
            # an offer
            prod_price = np.random.choice((prod_IDs[sale_prod],0),1,p=[0.99,0.01])[0]

            string = '''DECLARE @ins_var as TABLE (ID int)
            INSERT INTO dbo.Clients ([Start_Date],[Gender],[Country])
            OUTPUT inserted.ID INTO @ins_var
            VALUES ('{0}','{1}','{2}')
            INSERT INTO dbo.Sales ([Sale_Date],[Year_Month],[Client_ID],[Product_ID],[Paid])
            VALUES ('{0}', {3},(SELECT ID FROM @ins_var), {4}, {5})'''

            query = string.format(run_date,np.random.choice(genders,1,p=[0.4,0.6])[0],
                                  np.random.choice(countries,1)[0],year_month,sale_prod,prod_price)
                                  
            cursor.execute(query)
            i += 1

        else:
            # Existing customer sale

            # Get possible sale clients
            cursor.execute("SELECT ID FROM dbo.Clients")
            client_IDs = [x for (x,) in cursor.fetchall()]

            if client_IDs == []: pass

            else:
                # Get specific client
                cli_ID = np.random.choice(client_IDs, 1)[0]

                # Get possible sale products
                cursor.execute("SELECT ID, Price FROM dbo.Products")
                prod_IDs = {x: y for (x, y) in cursor.fetchall()}

                # Randomly getting a product ID for the sale we're generating
                sale_prod = np.random.choice(list(prod_IDs.keys()),1)[0]
                # Getting price of the product sold, sometimes (p=0.01) the product will cost 0€ as it will be
                # considered as an offer
                prod_price = np.random.choice((prod_IDs[sale_prod],0),1,p=[0.99,0.01])[0]

                string = '''INSERT INTO dbo.Sales ([Sale_Date],[Year_Month],[Client_ID],[Product_ID],[Paid]) 
                VALUES ('{0}', {1}, {2}, {3}, {4})'''

                query = string.format(run_date,year_month,cli_ID,sale_prod,prod_price)
                cursor.execute(query)
                i += 1

            # Randomly delete sales, we're considering that these were sales that eventually got cancelled
            # (we're considering p=0.001)
            delete_flag = np.random.choice(2,1,p=[0.999,0.001])

            if delete_flag == 0: pass
            else:
                # Get possible sales to delete
                cursor.execute("SELECT ID FROM dbo.Sales")
                sale_IDs = [x for (x,) in cursor.fetchall()]

                if sale_IDs == []: pass

                else:
                    # Get specific sale
                    sale_ID = np.random.choice(sale_IDs, 1)[0]

                    string = '''DECLARE @del_var TABLE (ID int, Sale_Date Datetime, Year_Month int)
                    DELETE FROM dbo.Sales OUTPUT deleted.ID, deleted.Sale_Date, deleted.Year_Month INTO @del_var
                    WHERE [ID] = {0}
                    INSERT INTO dbo.Removed ([Table], [ID], [Record_Date], [Year_Month], [Deleted_Date]) 
                    VALUES ('Sales',(SELECT ID FROM @del_var), (SELECT Sale_Date FROM @del_var), (SELECT Year_Month FROM @del_var), '{1}')'''

                    query = string.format(sale_ID,run_date)
                    cursor.execute(query)

            # Randomly apply payment changes on sales, we're considering that these were sales that got partially
            # refunded or were corrected later on
            pay_change_flag = np.random.choice(2,1,p=[0.99,0.01])

            if pay_change_flag == 0: pass
            else:
                # Get possible sales to update
                cursor.execute("SELECT ID FROM dbo.Sales")
                sale_IDs = [x for (x,) in cursor.fetchall()]

                if sale_IDs == []: pass

                else:
                    # Get specific sale
                    sale_ID = np.random.choice(sale_IDs, 1)[0]

                    string = '''UPDATE dbo.Sales
                    SET Paid = Paid*0.95, Updated_Date = '{1}'
                    WHERE [ID] = {0}'''

                    query = string.format(sale_ID,run_date)
                    cursor.execute(query)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    len_argv = len(sys.argv)

    # If the number of records to be generated has been specified when running the script, then that number will be
    # used, else the default number of 100 will be used instead
    if len_argv > 1:
        if type(int(sys.argv[1])) == int:
            no_recs = int(sys.argv[1])
        else:
            no_recs = 100
    else:
        no_recs = 100

    # If a date was specified when running the script then that date will be used instead of the current time, it must
    # be specified as the second argument after no. of records
    if len_argv == 3:
        rec_date = datetime.strptime(sys.argv[2],"%Y-%m-%d %H:%M:%S")
    else:
        now = datetime.now()
        rec_date = now.replace(microsecond=0)

    generate_data(no_recs, rec_date)

        


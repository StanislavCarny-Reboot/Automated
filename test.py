import psycopg2
import datetime 
from sqlalchemy import create_engine
import pandas as pd
import os 

database = os.environ.get("HEROKU_DB")
password = os.environ.get("HEROKU_PASS")
host = os.environ.get("HEROKU_HOST")
user = os.environ.get("HEROKU_USER")


test = pd.DataFrame({'Name':['runs']})
test['time'] = datetime.datetime.now()


# define connection
connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    user,
    password,
    host,
    database
)

def to_alchemy(df):
    """
    Using a dummy table to test this call library
    """
    engine = create_engine(connect)
    df.to_sql(
        'cron_test', 
        con=engine, 
        index=False, 
        if_exists='replace'
    )
    
    
to_alchemy(test)
    

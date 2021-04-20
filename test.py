import psycopg2
import datetime 
from sqlalchemy import create_engine
import pandas as pd
import os 

database = os.environ.get("HEROKU_DB")


test = pd.DataFrame({'Name':['runs']})
test['time'] = datetime.datetime.now()


# define connection
connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    'ziverzcxjkukah',
   '86be3e356b4890952d6dddefc0580644b8e262797eacd19390e90c55b6b66d9b',
    'ec2-54-72-155-238.eu-west-1.compute.amazonaws.com',
    database
    #'d77vu5pdkc80c5',
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
    

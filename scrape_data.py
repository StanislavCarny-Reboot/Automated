# import libraries
import numpy as np
import pandas as pd
import requests
import re 
import datetime
import psycopg2
from sqlalchemy import create_engine
import os 

### define sources for api calls

prefix = 'https://www.sreality.cz/api'

one_kk = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=2&category_type_cb=1&locality_region_id=10&per_page=999&tms=1616528564937'
one_plus_one = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=3&category_type_cb=1&locality_region_id=10&per_page=999&tms=1616528619889'
two_KK_7M = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=4&category_type_cb=1&czk_price_summary_order2=1%7C7000000&locality_region_id=10&per_page=999&tms=1616528426761'
two_KK_7M_plus = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=4&category_type_cb=1&czk_price_summary_order2=7000000%7C100000000&locality_region_id=10&per_page=999&tms=1615495719220'
two_plus_one = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=5&category_type_cb=1&locality_region_id=10&per_page=999&tms=1617900806824'
three_KK_10M = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=6&category_type_cb=1&czk_price_summary_order2=1%7C10000000&locality_region_id=10&per_page=999&tms=1619801573515'
three_KK_10M_plus = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=6&category_type_cb=1&czk_price_summary_order2=10000000%7C10000000000&locality_region_id=10&per_page=999&tms=1617447848438'
three_plus_one = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=7&category_type_cb=1&czk_price_summary_order2=1%7C10000000000&locality_region_id=10&per_page=999&tms=1619801137282'
four = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=8%7C9&category_type_cb=1&czk_price_summary_order2=1%7C10000000000&locality_region_id=10&per_page=999&tms=1617447981333'
five_and_more = 'https://www.sreality.cz/api/cs/v2/estates?category_main_cb=1&category_sub_cb=10%7C11%7C12%7C16&category_type_cb=1&czk_price_summary_order2=1%7C10000000000&locality_region_id=10&per_page=999&tms=1617448030932'

apis = [one_kk,one_plus_one,two_KK_7M,two_KK_7M_plus,two_plus_one,three_KK_10M,three_KK_10M_plus,three_plus_one,four,five_and_more]


#---------------------------------------------------------------------------------------

# define fn get estate details - getting deeper into json structure to get links for estate details

def get_urls(url):
    r = requests.get(url)
    details = pd.json_normalize(r.json()['_embedded']['estates'])
    details['urls'] = prefix + details['_links.self.href']
    details['call_content'] = details['urls'].apply(lambda x: requests.get(x))
    details['call_status'] = details['call_content'].apply(lambda x:x.status_code)
    details = details[details['call_status']==200]
    return details

# define fn to get furter information in esttate details

def get_list_of_items():
    global details
    items = [i.json() for i in details['call_content'] ]
    dff = pd.json_normalize(items)
    return dff


# unpacking details from the previous step

def unpack_item_details():
    items_unpacked = pd.DataFrame()
    for item,index in zip(dff['items'],dff.index):
        names = []
        values = []
        for i in item:

                names.append(i['name'])
                values.append(i['value'])
        temp_df = pd.DataFrame( data = [values],columns=names)  
        items_unpacked = items_unpacked.append(temp_df)
    return items_unpacked


# cleaning dataframe

def clean_df():
    global items_unpacked
    features = ['Aktualizace','Stavba','Vlastnictví','Podlaží','Užitná plocha']#,'Balkón']
    items_unpacked = items_unpacked[features]
    items_unpacked.reset_index(drop=True,inplace=True)
    features = ['map.lat','map.lon','locality.value']
    
    global dff
    dff = dff[features]
    
    global details
    features = ['hash_id','locality','name','price','_links.self.href','_links.images']
    details = details[features]
    
    # unpack image urls
    images = [i[0]['href'] for i in details['_links.images']]
    details['image_url'] = images
    details.drop('_links.images',axis=1,inplace=True)
    
    # final merge
    merged = pd.merge(dff,items_unpacked, left_index=True, right_index=True)
    join = pd.merge(details,merged,left_index =True, right_index=True)
    return join


# final adjustments for the final dataset

def add_columns():
    global join
    final['rooms'] = final['name'].apply(lambda x :re.search('[0-9]+\+kk|[0-9]+\+[0-9]',x)[0] if re.search('[0-9]+\+kk|[0-9]+\+[0-9]',x) is not None else 'atyp')
    final['quarter'] = final['locality'].apply(lambda x : x.split('- ')[-1]) 
    final['Užitná plocha'] = final['Užitná plocha'].astype('int')
    final['Average per Quarter'] = final.groupby(['quarter','rooms'])['price'].transform(np.mean)
    final['Podlaží'] = final['Podlaží'].apply(lambda x : str(x).replace('přízemí',"0."))
    final['Podlaží'] = final['Podlaží'].replace('nan','0')
    final['Floor'] = final['Podlaží'].apply(lambda x : re.search('[0-9]',x)[0] if re.search('[0-9]',x)[0] is not None else '99')
    final['ScrapeDate'] = datetime.datetime.now()
    final['map.lat'].fillna('50.0939816',inplace=True) # replacting with generic lat
    final['map.lon'].fillna('14.4105983',inplace=True) # replacting with generic lon



    
 # declare connecting variables from secrets
database = os.environ.get("HEROKU_DB")
password = os.environ.get("HEROKU_PASS")
host = os.environ.get("HEROKU_HOST")
user = os.environ.get("HEROKU_USER")



# define connection
connect = "postgresql+psycopg2://%s:%s@%s:5432/%s" % (
    user,
    password,
    host,
    database
)

# fn connect to Heraku and create or replace final dataframe

def create_table_heroku(df):

    engine = create_engine(connect)
    df.to_sql(
        'prg_apartments', 
        con=engine, 
        index=False, 
        if_exists='replace'
    )
    
#----------------------------------------------------------


# calling all defined functions

final = pd.DataFrame()
for a in apis:
    details = get_urls(a)
    dff = get_list_of_items()
    items_unpacked = unpack_item_details()
    join = clean_df()
    final = final.append(join)
    add_columns()
    
    
final.drop('locality.value',axis=1,inplace=True)
final.drop('Podlaží',axis=1,inplace=True)
    
# rename columns for the DB
new_column_names = ['hash_id', 'locality', 'name', 'price', 'sreality_link', 'image_url',
       'lat', 'lon', 'refresh_date', 'material', 'ownership',
       'square_meters', 'rooms', 'quarter', 'avg_per_quarter', 'Floor',
       'ScrapeDate']
final.columns = new_column_names
final['lat'] = final['lat'].astype('float')
final['lon'] = final['lon'].astype('float')
final['price'] = final['price'].apply(lambda x:1 if x==0 else x)
final['avg_per_quarter'] = final['avg_per_quarter'].apply(lambda x:1 if x==0 else x)

# push data to DB
create_table_heroku(final)

    

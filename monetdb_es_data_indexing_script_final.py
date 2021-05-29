#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 15:58:50 2021

@author: YATRAONLINE\v-vivek.singh
"""

import numpy as np
import pandas as pd
import pymonetdb

def monet_connection():
    connection = pymonetdb.connect(
    host ="127.0.0.1",
    port= "50001",
    database = "voc",
    username="monetdb",
    password="monetdb")
    cur= connection.cursor() 
    return cur  



//Putting data from monetdb to pandas dataframe 
cur= monet_connection()
cur.execute("select * from faqinternal;")
table_rows = cur.fetchall()
df = pd.DataFrame(table_rows)

//fetching database table columns
cur= monet_connection()
cur.execute("select col.name from sys.tables as tab JOIN sys.columns as col ON tab.id=col.table_id WHERE tab.name='faqinternal';")
res= cur.fetchall()
columnNames = [i[0] for i in res]
df.columns = columnNames

/////////////////////////////////////////////////////////////////////
#index_name_test = "loan_prediction_test"
#doc_type_test = "av-lp_test"
Cleaning the dataset
1. cleaning the dates to the values elastic search accepts
2. Ovoiding other blank values(we'll swap the blank/nan in strings with other') 

/////////////////////////////////////////////////////////////////////
#Cleaning for dates
from datetime import datetime
def safe_date(date_value):
    return (
        pd.to_datetime(date_value) if not pd.isna(date_value)
            else  datetime(1970,1,1,0,0)
    )
df['lastmodified'] = df['lastmodified'].apply(safe_date)
df['createdat'] = df['createdat'].apply(safe_date)

#Cleaning for other blank strings
def safe_value(field_val):
    return field_val if not pd.isna(field_val) else "Other"

df['modifiedby'] = df['modifiedby'].apply(safe_value)
df['createdby'] = df['createdby'].apply(safe_value)
df['phrasing'] = df['phrasing'].apply(safe_value)
df['answer'] = df['answer'].apply(safe_value)
df['labels'] = df['labels'].apply(safe_value)

#Cleaning for integer column datatype
df['id'] = df['id'].replace(np.nan, 0)
df['likes'] = df['likes'].replace(np.nan, 0)
df['dislikes'] = df['dislikes'].replace(np.nan, 0)
df['hits'] = df['hits'].replace(np.nan, 0)
df['active'] = df['active'].replace(np.nan, 0)


df.dtypes
df.isnull().sum()

#Transforming data into dictionary format to be imported to elastic search
use_these_keys = ['question', 'answer', 'index_id', 'labels','lastmodified','modifiedby','createdat','phrasing','likes','dislikes','hits','active']
def filterKeys(document):
    return {key: document[key] for key in use_these_keys }

/////////////////////////////////////////////////////////////////////
#Sending data to elastic search using python /////////////////////////////////////////////////////////////////////client and helpers
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
es_client = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}], connection_class=RequestsHttpConnection, http_compress=True)
def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": 'using_espandas_faq_internal_test6',           #creating an index with "your_index" as an index name
                "_type": "_doc",                  #Can be understood as tables, logically separating the data, but in future the types in the elastci search will be removed
                "_id" : f"{document['id']}",      #adding index variable from iterrows
                "_source": filterKeys(document),  #document to be saved
              }
    #raise StopIteration
    
    
your_dataframe=df
helpers.bulk(es_client, doc_generator(your_dataframe))

#Removing Previous unordered id and making a new one
df.drop(['id'], axis = 1, inplace = True)
df.columns[0] = 'id'
df['id'] = df.index
/////////////////////////////////////////////////////////////////////

df['labels'][3]

df = df.astype(str)

////////////////////////////////////////////////////////////////////
Ingesting pandas dataframe to ES as a whole
import espandas
es = Elasticsearch(hosts=[{"host": "localhost", "port": 9200}], connection_class=RequestsHttpConnection, http_compress=True)

data= df
data['indexId']= 'ind' + data.index.astype(str)


INDEX = 'using_espandas_faq_internal'
TYPE = '_doc'
esp = espandas.Espandas()
esp.es_write(data, INDEX, TYPE)

k = [‘ind’+str(i) for i in 10]
res = esp.es_read(k, INDEX, TYPE)




for success, info in helpers.parallel_bulk(es, doc_generator(your_dataframe)):
    if not success: print('Doc failed', info)

//////////////////////////////////////////////////////////////////////////////////
from elasticsearch.helpers import parallel_bulk
import espandas as es

header = data.columns
bulk_data = []
index_name ='using_espandas_faq_internal9'
doc_type ='_doc'

def genereate_actions(data):
    for i in range(len(data)):
        source_dict = {}
        row = data.iloc[i]
        for k in header:
            source_dict[k] = str(row[k])
        yield {
            '_op_type': 'index',
            '_index': index_name,
            '_type': doc_type,
            '_source': source_dict
        }
        
data= df
for success, info in parallel_bulk(es, genereate_actions(data)):
    if not success: print('Doc failed', info)






























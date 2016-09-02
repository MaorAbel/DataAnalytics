# -*- coding: utf-8 -*-
"""
Created on 01/09/16

@author: adir

"""


from pymongo import MongoClient
import pandas as pd
from time import time
import fused_db as fdb
from dbase_utils import IMDB_ID_KEY


DB_NAME = 'mba_python_moviedb'
COLL_NAME = 'movies'

# URL = None
URL = 'mongodb://adir:Qweasd0987@ds019766.mlab.com:19766/' + DB_NAME


def _get_movies_mongo(url=URL, db_name=DB_NAME, coll_name=COLL_NAME):
    # get client
    client = MongoClient(url)

    # get db
    db = client[db_name]

    # get collection
    return db[coll_name]


def store_to_mongo(**kwargs):

    # get collection
    mov_col = _get_movies_mongo(**kwargs)

    # get CSV-based data and store it

    df = fdb.load_all_years()

    # store to collection
    list_ = df.to_dict('records')
    # couldn't find a clean way to add the index as key to list of records
    for ind, row in enumerate(list_):
        row[IMDB_ID_KEY] = df.index[ind]
    mov_col.insert(list_)

    # using the index as key - not so good for fetching
    # index = df.to_dict('index')
    # list_ = [{key: val} for key, val in index.iteritems()

    print mov_col


def load_mongo_db(**kwargs):

    # get collection
    mov_col = _get_movies_mongo(**kwargs)

    cursor = mov_col.find({})

    t0 = time()
    list_ = list(cursor)
    print 'Fetched', len(list_), 'records in', time() - t0

    df = pd.DataFrame.from_records(list_, index=IMDB_ID_KEY)

    return df


if __name__ == '__main__':
    # store_to_mongo()
    load_mongo_db()

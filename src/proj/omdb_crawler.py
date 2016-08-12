# -*- coding: utf-8 -*-
"""
Created on 05/08/16

@author: adir

"""


import os
import omdb
from dbase_utils import _DBBase, _df_from_records, _get_releavent_path_year,\
    load_from_csv, DB_ROOT


OMDB_PATH = os.path.join(DB_ROOT, 'OMDB')
if not os.path.isdir(OMDB_PATH):
    os.mkdir(OMDB_PATH)


class OMDBInterface(object):

    def __init__(self):
        pass

    def load(self, id):
        if not isinstance(id, (str, unicode)):
            id = 'tt' + format(id, '07')
        return dict(omdb.imdbid(id, tomatoes=True).items())


class OMDB(_DBBase):

    type_ = 'omdb'

    def __init__(self, data, **kwargs):
        super(OMDB, self).__init__(data, **kwargs)

    @staticmethod
    def from_records(list_, **kwargs):
        return OMDB(_df_from_records(list_), **kwargs)

    @staticmethod
    def from_csv(path):
        return load_from_csv(path, OMDB)

    @staticmethod
    def from_csv_def(year):
        path = _get_releavent_path_year(OMDB_PATH, year)
        return OMDB.from_csv(path)

    def _get_def_path(self):
        return os.path.join(OMDB_PATH, self._get_def_name())

    def extract_important(self):
        subset = [
            'actors',
            'country',
            'director',
            'dvd',
            'imdb_rating',
            'imdb_votes',
            'language',
            'metascore',
            'plot',
            'poster',
            'production',
            'rated',
            'released',
            'runtime',
            'title',
            'tomato_consensus',
            'tomato_image',
            'tomato_meter',
            'tomato_rating',
            'tomato_reviews',
            'tomato_rotten',
            'tomato_user_meter',
            'tomato_user_rating',
            'tomato_user_reviews',
            'writer',
            'year'
        ]
        return self.data[subset]

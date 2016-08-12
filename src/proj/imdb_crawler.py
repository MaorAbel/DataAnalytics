# -*- coding: utf-8 -*-
"""
Created on 06/08/16

@author: adir

"""


import os
from imdb import IMDb
from dbase_utils import _DBBase, _df_from_records, _get_releavent_path_year,\
    load_from_csv, DB_ROOT



IMDB_PATH = os.path.join(DB_ROOT, 'IMDB')
if not os.path.isdir(IMDB_PATH):
    os.mkdir(IMDB_PATH)


class IMDBInterface(object):

    info_subset_ = [
        'rating',
        'runtimes',
        'year',
        'long imdb canonical title',
        'votes',
        'title',
        'mpaa',
        'top 250 rank',
        'kind',
        'long imdb title',
        'country codes',
        'language codes',
        'cover url',
        # 'director',
        'genres',
        'plot outline',
        'plot',
        'full-size cover url',
        'canonical title',
        'smart long imdb canonical title',
        'smart canonical title'
        ]

    def __init__(self):
        self.ia = IMDb()

    def load(self, id):
        if not isinstance(id, (str, unicode)):
            id = format(id, '07')
        elif id.startswith('tt'):
            id = id[2:]
        return dict(self.ia.get_movie(id).items()), id

    def load_small(self, id):
        out, id = self.load(id)
        dict_ = {}
        dict_['imdb_id'] = 'tt' + id
        for k in self.info_subset_:
            if k in out:
                dict_[k] = out[k]
            elif k == 'top 250 rank':
                dict_[k] = -1
            else:
                print k, 'not in', id
                dict_[k] = 'N/A'
        return dict_


class IMDB(_DBBase):

    type_ = 'imdb'

    def __init__(self, data, **kwargs):
        super(IMDB, self).__init__(data, **kwargs)

    @staticmethod
    def from_records(list_, **kwargs):
        return IMDB(_df_from_records(list_), **kwargs)

    @staticmethod
    def from_csv(path):
        return load_from_csv(path, IMDB)

    @staticmethod
    def from_csv_def(year):
        path = _get_releavent_path_year(IMDB_PATH, year)
        return IMDB.from_csv(path)

    def _get_def_path(self):
        return os.path.join(IMDB_PATH, self._get_def_name())
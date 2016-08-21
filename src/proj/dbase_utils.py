# -*- coding: utf-8 -*-
"""
Created on 06/08/16

@author: adir

"""

import os
from glob import glob
import pandas as pd


DB_ROOT = 'DBase'
if not os.path.isdir(DB_ROOT):
    os.mkdir(DB_ROOT)

TMDB_PATH = os.path.join(DB_ROOT, 'TMDB')
if not os.path.isdir(TMDB_PATH):
    os.mkdir(TMDB_PATH)

LAST_YEAR = 2016
N_YEARS_TOTAL = 100


def _get_name_and_year(path):
    if os.path.isfile(path):
        name = os.path.basename(path)
        return name, int(name[-8:-4])
    else:
        print 'No such file', path
        return None, None


def _df_from_records(list_of_dicts, index_str='imdb_id'):
    return pd.DataFrame.from_records(list_of_dicts, index=index_str)


def _df_from_csv(path, index_col='imdb_id'):
    df = pd.DataFrame.from_csv(path, index_col=index_col)
    return df[~pd.isnull(df.index)]


def _get_releavent_path(path, key):
    list_ = glob(os.path.join(path, key))
    if len(list_) == 0:
        print 'No files in', path, 'with rule', key
        return None
    list_.sort()
    return list_[-1]


def _get_releavent_path_year(path, year, pref=''):
    key = pref + '*' + str(year) + '.csv'
    return _get_releavent_path(path, key)


def load_from_csv(path, clss):
    _, year = _get_name_and_year(path)
    if year is not None:
        return clss(_df_from_csv(path), year=year, path=path)
    else:
        return None


class _DBBase(object):

    type_ = 'base'

    def __init__(self, data, year, path=''):
        self.data = data
        self.year = year
        self.path = path

    def _get_def_name(self):
        return self.type_ + '_nmovies_' + format(self.n_movies, '04') + \
            '_' + str(self.year) +'.csv'

    def _get_def_path(self):
        return os.path.join(TMDB_PATH, self._get_def_name())

    def _get_n_movies(self, max_records=None):
        if max_records is None:
            max_records = self.n_movies
        return max_records

    def to_csv(self, path=None):
        if path is None:
            path = self._get_def_path()
        self.data.to_csv(path, encoding='utf8')
        return path

    def _get_inds(self, max_records=None):
        max_records = self._get_n_movies(max_records)
        return self.data.index[:max_records]

    @staticmethod
    def _print_err(id, suff):
        print 'Failed to get movie', id, 'from', suff

    @property
    def n_movies(self):
        return len(self.data)

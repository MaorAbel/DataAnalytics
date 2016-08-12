# -*- coding: utf-8 -*-
"""
Created on 06/08/16

@author: adir

"""


import os
import pandas as pd


from dbase_utils import _DBBase, _df_from_records, _get_releavent_path_year,\
    load_from_csv, DB_ROOT, LAST_YEAR, N_YEARS_TOTAL


TTL_COL = 'title'
DATE_COL = 'released'
YEAR_COL = 'year'

DEF_VIEW_COLS = [
    'title',
    'released'
    'imdb_rating',
    'imdb_votes',
    'metascore',
    'tomato_meter',
    'tomato_rating',
    'tomato_reviews',
    'tomato_rotten',
    'tomato_user_meter',
    'tomato_user_rating',
    'tomato_user_reviews',
    'tmdb_popularity',
    'tmdb_vote_average',
    'tmdb_vote_count',
    'budget',
    'revenue',
    'roi'
]


def get_roi_col(df):
    df['roi(%)'] = (df['revenue'] / df['budget'] - 1) * 100.
    return df


class FusedDB(_DBBase):

    type_ = 'fused'

    def __init__(self, data, **kwargs):
        super(FusedDB, self).__init__(data, **kwargs)

    @staticmethod
    def from_records(list_, **kwargs):
        return FusedDB(_df_from_records(list_), **kwargs)

    @staticmethod
    def from_csv(path):
        return load_from_csv(path, FusedDB)

    @staticmethod
    def from_csv_def(year):
        path = _get_releavent_path_year(DB_ROOT, year)
        return FusedDB.from_csv(path)

    def _get_def_path(self):
        return os.path.join(DB_ROOT, self._get_def_name())


def load_all_years(path=DB_ROOT, year_stop=LAST_YEAR, year_start=None,
                   **kwargs):
    year_stop = int(year_stop)
    if year_start is None:
        year_start = year_stop - N_YEARS_TOTAL + 1
    else:
        year_start = int(year_start)
    dfs = []
    for year in range(year_start, year_stop + 1):
        res = _get_releavent_path_year(path, year, FusedDB.type_)
        if res is not None:
            dfs += [pd.read_csv(res, index_col=0)]
    df = pd.concat(dfs, axis=0)
    df = get_roi_col(df)
    return df


def print_subset(df, sort_by=['tomato_user_meter'],
                 columns=['metascore', 'revenue', 'budget', 'roi(%)'],
                 n_top=50, **kwargs):
    n_top = int(n_top)
    if isinstance(sort_by, str):
        sort_by = [sort_by]
    diff_list = list(set(columns).difference(sort_by))
    cols_ = [x for x in columns if x in diff_list]
    columns = [TTL_COL, YEAR_COL] + sort_by + cols_
    df2 = df.sort(ascending=False, columns=sort_by, inplace=False)
    df2.index = pd.Index(range(1, len(df2)+1))
    print df2.loc[0:n_top, columns]


def load_and_print(**kwargs):
    df = load_all_years(**kwargs)
    print_subset(df, **kwargs)


if __name__ == '__main__':

    if len(os.sys.argv) > 1:
        params = {}
        for arg in os.sys.argv[1:]:
            key, value = arg.split('=')
            params[key] = value
        load_and_print(**params)

    else:
        # df = load_all_years()
        # df.to_csv(os.path.join(DB_ROOT, 'tmp.csv'))
        # print_subset(df)
        load_and_print(n_top=20, year_start=2006, sort_by='metascore')
        print 'Done.'

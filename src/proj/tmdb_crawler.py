# -*- coding: utf-8 -*-
"""
Created on 05/08/16

@author: adir

"""


import os
import pandas as pd
from tqdm import tqdm
import tmdbsimple as tmdb
from dbase_utils import _get_name_and_year, _df_from_csv, TMDB_PATH, _DBBase,\
    _df_from_records, _get_releavent_path_year, load_from_csv


API_KEY = r'139cb51bf840b3fe8abf0e50def88a4b'


class TMDBInterface(object):

    def __init__(self, api_key=API_KEY):
        print 'TMDBInterface setting API Key to', api_key
        tmdb.API_KEY = API_KEY

    def load(self, id):
        info = tmdb.Movies(id).info()
        info['genres'] = [x['name'] for x in info['genres']]
        return info


class TMDBSmall(_DBBase):

    type_ = 'tmdb_small'

    def __init__(self, data, n_pages, **kwargs):
        super(TMDBSmall, self).__init__(data, **kwargs)
        self.n_pages = n_pages

    @staticmethod
    def from_records(list_, *args, **kwargs):
        return TMDBSmall(pd.DataFrame.from_records(list_, index='id'),
                         *args, **kwargs)

    @staticmethod
    def from_csv(path):
        name, year = _get_name_and_year(path)
        if name is not None:
            n_pages = int(name[-12:-9])
            return TMDBSmall(_df_from_csv(path, 'tmdb_id'), n_pages, year=year,
                             path=path)
        else:
            return None

    @staticmethod
    def from_csv_def(year):
        path = _get_releavent_path_year(TMDB_PATH, year, TMDBSmall.type_)
        return TMDBSmall.from_csv(path)

    def _get_def_path(self):
        return os.path.join(TMDB_PATH, self.type_ +
                            '_npages_' + format(self.n_pages, '03') +
                            '_' + str(self.year) + '.csv')

    def _append_pref(self, keys):
        return {key: 'tmdb_' + key for key in keys}

    def to_large(self, max_records=None):
        tmdb_inter = TMDBInterface()
        ids = self._get_inds(max_records)
        list_ = []
        print 'TMDB is fetching first', len(ids), 'records from', self.year
        for id in tqdm(ids):
            try:
                res = tmdb_inter.load(id)
                if 'imdb_id' in res and res['imdb_id'] is not None and len(
                        res['imdb_id']) > 0:
                    list_ += [res]
                else:
                    print id, 'did not return and IMDb index, dropping it.'
            except:
                print self._print_err(id, 'TMDB')
        print 'Done'
        tmdb_large = TMDBLarge.from_records(list_, year=self.year)
        keys_rename = [
            'id',
            'popularity',
            'vote_average',
            'vote_count'
        ]
        tmdb_large.data.rename(columns=self._append_pref(keys_rename),
                               inplace=True)
        return tmdb_large


class TMDBLarge(_DBBase):

    type_ = 'tmdb_large'

    def __init__(self, data, **kwargs):
        super(TMDBLarge, self).__init__(data, **kwargs)

    @staticmethod
    def from_records(list_, **kwargs):
        return TMDBLarge(_df_from_records(list_), **kwargs)

    @staticmethod
    def from_csv(path):
        return load_from_csv(path, TMDBLarge)

    @staticmethod
    def from_csv_def(year):
        path = _get_releavent_path_year(TMDB_PATH, year, TMDBLarge.type_)
        return TMDBLarge.from_csv(path)

    def _get_def_path(self):
        return os.path.join(TMDB_PATH, self._get_def_name())

    def get_imdb_id(self, max_records=None):
        max_records = self._get_n_movies(max_records)
        return self.data.imdb_id[:max_records]

    def extract_important(self):
        subset = [
            'budget',
            'genres',
            'homepage',
            'tmdb_id',
            'overview',
            'tmdb_popularity',
            'revenue',
            'tagline',
            'tmdb_vote_average',
            'tmdb_vote_count'
        ]
        return self.data[subset]

    def query_omdb(self, max_records=None):
        from omdb_crawler import OMDB, OMDBInterface
        omdb_inter = OMDBInterface()
        ids = self._get_inds(max_records)
        list_ = []
        print 'OMDB is fetching first', len(ids), 'records from', self.year
        for id in tqdm(ids):
            try:
                list_ += [omdb_inter.load(id)]
            except:
                print self._print_err(id, 'OMDB')
        print 'Done'
        return OMDB.from_records(list_, year=self.year)

    def query_imdb(self, max_records=None):
        from imdb_crawler import IMDB, IMDBInterface
        imdb_inter = IMDBInterface()
        ids = self._get_inds(max_records)
        list_ = []
        print 'IMDB is fetching first', len(ids), 'records from', self.year
        for id in tqdm(ids):
            try:
                list_ += [imdb_inter.load_small(id)]
            except:
                print self._print_err(id, 'IMDB')
        print 'Done'
        return IMDB.from_records(list_, year=self.year)

    def query_wikidb(self, max_records=None, **kwargs):
        from wikipedia_crawler import WIKIDB, WIKIInterface
        wiki_inter = WIKIInterface(**kwargs)
        max_records = self._get_n_movies(max_records)
        list_ = []
        print 'Wikipedia is fetching first', max_records, 'records from', \
            self.year
        for ind in tqdm(range(max_records)):
            title = self.data.title[ind]
            id = self.data.index[ind]
            try:
                res = wiki_inter.load(title, id)
                if res == {}:
                    print self._print_err(id, 'Wikipedia')
                else:
                    list_ += [res]
            except:
                print self._print_err(id, 'Wikipedia')
        print 'Done'
        return WIKIDB.from_records(list_, year=self.year)

    def query_and_save_all(self, do_omdb=True, do_imdb=False,
                           do_wiki=True, load_wiki_pages=False, **kwargs):
        if do_omdb:
            self.query_omdb(**kwargs).to_csv()
        if do_imdb:
            self.query_imdb(**kwargs).to_csv()
        if do_wiki:
            self.query_wikidb(load_wiki_pages=load_wiki_pages,
                              **kwargs).to_csv()


if __name__ == '__main__':

    path = os.path.join(TMDB_PATH, 'tmdb_small_npages_010_2015.csv')
    dframe = TMDBSmall.from_csv(path)
    dframe_full = dframe.to_large(20)

    # path = os.path.join(TMDB_PATH, 'tmdb_large_nmovies_020_2015.csv')
    # dframe = TMDBLarge.from_csv(path)
    # dframe_full = dframe.query_omdb(20)
    # dframe_full = dframe.query_imdb(5)
    # dframe_full = dframe.query_wikidb(20)

    dframe_full.to_csv()
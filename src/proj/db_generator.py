# -*- coding: utf-8 -*-
"""
Created on 05/08/16

@author: adir

"""


import os
from urllib2 import Request, urlopen
import json
from tqdm import tqdm
import datetime as dt
import pandas as pd
from dbase_utils import TMDB_PATH, LAST_YEAR
from tmdb_crawler import API_KEY, TMDBSmall, TMDBLarge
from omdb_crawler import OMDB
from wikipedia_crawler import WIKIDB
from fused_db import FusedDB


TMDB_QUERY_PREF = r'http://api.themoviedb.org/3/discover/movie?'


class TMDBSearcher(object):

    def __init__(self, year=None, api_key=API_KEY, n_top=1000):
        if year is None:
            year = dt.datetime.today().date().year
        self.year = year
        self.api_key = api_key
        list_, self.n_pages = self.tmdb_query_page()
        self.n_mov_per_page = len(list_)
        self.n_pages = int(n_top / float(self.n_mov_per_page) + 0.5)
        print 'TMDB has', self.n_pages, 'pages for', year

    def tmdb_query_page(self, page=1):
        querry = TMDB_QUERY_PREF + \
                 'primary_release_year=' + str(self.year) + \
                 '&page=' + str(page) + \
                 '&api_key=' + self.api_key + \
                 '&language=en' + \
                 '&sort_by=vote_count.desc'
        request = Request(querry)
        response_str = urlopen(request).read()
        response_parsed = json.loads(response_str)
        return response_parsed['results'], response_parsed['total_pages']

    def tmdb_query_pages(self, n_pages=None):
        list_ = []
        if n_pages is None:
            n_pages = self.n_pages
        print 'TMDB is fetching top', n_pages, 'pages from', self.year
        for page in tqdm(range(1, n_pages + 1)):
            try:
                data, _ = self.tmdb_query_page(page)
                list_ += data
            except:
                print 'Failed to query page', page
        return list_, n_pages

    def query_to_dframe(self,  n_pages=None):
        list_, n_pages = self.tmdb_query_pages(n_pages=n_pages)
        print 'Fetched', len(list_), 'movies from TMDB'
        tmdb_small = TMDBSmall.from_records(list_, n_pages,
                                            year=self.year, )
        tmdb_small.data.index.rename('tmdb_id', inplace=True)
        return tmdb_small

    def query_and_save(self, *args, **kwargs):
        return self.query_to_dframe(*args, **kwargs).to_csv()


def save_tmdb_large(path, max_records=None):
    if not os.path.isfile(path):
        path = os.path.join(TMDB_PATH, path)
    dframe = TMDBSmall.from_csv(path)
    dframe_full = dframe.to_large(max_records)
    return dframe_full.to_csv()


def save_all_non_tmdb(path, max_records=None, **kwargs):
    if not os.path.isfile(path):
        path = os.path.join(TMDB_PATH, path)
    dframe_tmdb = TMDBLarge.from_csv(path)
    dframe_tmdb.query_and_save_all(max_records=max_records, **kwargs)


def save_all_intermediary(path, max_records=None, **kwargs):
    path_tmdb = save_tmdb_large(path, max_records)
    save_all_non_tmdb(path_tmdb, **kwargs)


def _get_intersection(list1, list2):
    list3 = list(set(list1).intersection(set(list2)))
    return [x.strip() for x in list3]


def fuse_year(year, do_tmdb=True, do_wiki=False):

    print 'Fusing', year, '...'
    omdb_df = OMDB.from_csv_def(year)
    print omdb_df.path
    df_base = omdb_df.extract_important()
    df_base = df_base[~pd.isnull(df_base.index)]

    df_tmdb = None
    if do_tmdb:
        tmdb_df = TMDBLarge.from_csv_def(year)
        if tmdb_df is not None:
            print tmdb_df.path
            df_tmdb = tmdb_df.extract_important()
            df_tmdb = df_tmdb[~pd.isnull(df_tmdb.index)]
        else:
            print 'Failed to add data for TMDB for year', year

    df_wiki = None
    if do_wiki:
        wiki_df = WIKIDB.from_csv_def(year)
        if wiki_df is not None:
            print wiki_df.path
            df_wiki = wiki_df.data
        else:
            print 'Failed to add data for IMDB for year', year

    joined = _get_intersection(df_base.index, df_tmdb.index)

    if df_wiki is not None:
        joined = _get_intersection(joined, df_wiki.index)

    df_base = df_base.loc[joined]
    df_tmdb = df_tmdb.loc[joined]

    if df_wiki is not None:
        df_wiki = df_wiki.loc[joined]

    df_fused = pd.concat([df_base, df_tmdb, df_wiki], axis=1)
    fused_db = FusedDB(df_fused, year=year)
    return fused_db.to_csv()


def run_year(year=LAST_YEAR, n_top=1000, load_wiki_pages=False, do_imdb=False):

    # create small database to access with to TMDB
    searcher = TMDBSearcher(year, n_top=n_top)
    tmdb_small_path = searcher.query_and_save()

    # save all intermediary results
    save_all_intermediary(
        tmdb_small_path,
        do_omdb=True,
        do_imdb=do_imdb,
        do_wiki=False,
        load_wiki_pages=load_wiki_pages
    )

    # fuse intermediary databases
    fuse_year(year)


def run_years(year_stop=LAST_YEAR, year_start=None, **kwargs):
    if year_start is None:
        year_start = year_stop - 9
    for year in range(year_start, year_stop+1):
        try:
            run_year(year, **kwargs)
        except:
            print 'Creating a database for year', year, 'failed.'


if __name__ == '__main__':

    if len(os.sys.argv) > 1:

        params = {}
        for arg in os.sys.argv[1:]:
            key, value = arg.split('=')
            params[key] = int(value)

        run_years(**params)

    else:
        # crawler = TMDBSearcher(2015)
        # crawler.query_and_save(n_pages_max=10)
        # crawler.query_and_save()

        # save_all_intermediary('tmdb_small_npages_010_2015.csv', max_records=20,
        #                   do_imdb=False, do_omdb=False, do_wiki=False)

        # save_all_non_tmdb('tmdb_large_nmovies_020_2015.csv',
        #                   do_imdb=True, do_omdb=False, do_wiki=True)

        # df_fused = fuse_year(2015)
        # df_fused.to_csv('tmp2.csv')

        # run_year(2015, n_top=60)

        run_years(year_start=2014, year_stop=LAST_YEAR, n_top=60)

    print 'Done'


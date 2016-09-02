# -*- coding: utf-8 -*-
"""
Created on 06/08/16

@author: adir

"""


import os
import wikipedia as wiki
from dbase_utils import _DBBase, _df_from_records, _get_releavent_path_year,\
    load_from_csv, DB_ROOT, IMDB_ID_KEY



WIKI_PATH = os.path.join(DB_ROOT, 'WIKI')
if not os.path.isdir(WIKI_PATH):
    os.mkdir(WIKI_PATH)



class WIKIInterface(object):

    def __init__(self, load_wiki_pages=True):
        self.load_pages = load_wiki_pages

    def load(self, title, id):
        try:
            res = wiki.search(title)
            # take first one
            if 'disambiguation' in res[0]:
                res = res[1:]
            name = res[0]
            for record in res:
                if 'film' in record:
                    name = record
                    break
            if title not in name:
                for record in res:
                    if title in record:
                        name = record
                        break
            dict_ = {IMDB_ID_KEY: id, 'wiki_page': name}
            if self.load_pages:
                page = wiki.page(name)
                dict_['wiki_pageid'] = page.pageid
                dict_['wiki_url'] = page.url
                dict_['wiki_summary'] = page.summary
            return dict_
        except:
            print 'Wikiepedia search of', title, 'failed'
            return {}


class WIKIDB(_DBBase):

    type_ = 'wiki'

    def __init__(self, data, **kwargs):
        super(WIKIDB, self).__init__(data, **kwargs)

    @staticmethod
    def from_records(list_, **kwargs):
        return WIKIDB(_df_from_records(list_), **kwargs)

    @staticmethod
    def from_csv(path):
        return load_from_csv(path, WIKIDB)

    @staticmethod
    def from_csv_def(year):
        path = _get_releavent_path_year(WIKI_PATH, year)
        return WIKIDB.from_csv(path)

    def _get_def_path(self):
        return os.path.join(WIKI_PATH, self._get_def_name())
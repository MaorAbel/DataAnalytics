"""
Created on Jul 10, 2016

@author: MAbel
"""

from src.proj.input.InputInterface import InputInterface


class DummyInputImpl(InputInterface):

        def load(self):
            print("load of Dummy Test")
            return "data string input"

        def convertToDataMiningEntity(self, data):
                print("convert of Dummy Test")
                return data + " end convert!!!!!"

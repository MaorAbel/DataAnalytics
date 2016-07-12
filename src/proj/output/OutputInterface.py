"""
Created on Jul 10, 2016

@author: MAbel
"""
import abc


class OutputInterface(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def generate_report(self):
        """create report"""
        return;


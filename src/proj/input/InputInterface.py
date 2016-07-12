

'''
Created on Jul 10, 2016

@author: MAbel
'''
import abc
class InputInterface(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def load(self):
        """Retrieve data from the input source and return an object."""
        return
    
    @abc.abstractmethod
    def convertToDataMiningEntity(self, data):
        """convert the data to data mining entity."""
        return 
    
    def getInput(self):
        input = self.load();
        return self.convertToDataMiningEntity(input);
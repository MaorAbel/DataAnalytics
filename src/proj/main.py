"""
Created on Jul 8, 2016

@author: MAbel
"""
from src.proj.manager.DataManager import DataManager
from src.proj.utilities import FuncThread

if __name__ == '__main__':
    dtd_manager = DataManager()

    ''' Running input and process flow'''
    t1 = FuncThread(dtd_manager.start_data_flow())
    t1.start()
    t1.join()

    # ''' Running output flow'''
    # t2 = FuncThread(dtd_manager.generate_reports())
    # t2.start()
    # t2.join()




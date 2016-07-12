"""
Created on Jul 10, 2016

@author: MAbel
"""
from src.proj.dao import DBUploader
from src.proj.input.impl.DummyInputImpl import DummyInputImpl
import time
from src.proj.dao.DBUploader import DBUploader


class DataManager:
    """
    This class contains all the input, stored DB and ouput modules. 
    """
    inputsList = []
    dbUploader = DBUploader()
    outputList = []
    
    def __init__(self):
        """
        Constructor
        """
        self.init_input_list()
        
    def do_input_process(self):
        print("Manager do input process")
        entities_list = []
        for idx in range(0, len(self.inputsList)):
            curr_input = self.inputsList[idx]
            entities_from_input = curr_input.getInput()
            print(entities_from_input)
            entities_list.append(entities_from_input)
        return entities_list
        
    def start_data_flow(self):
        print("Start Data Flow! sampling every 10 seconds")
        while True:
            entities_list = self.do_input_process()
            self.dbUploader.insert_data_to_db(entities_list)
            time.sleep(10)

    def generate_reports(self):
        print("Start generating reports! sampling every 60 minutes")
        while True:
            for idx in range(0, len(self.outputList)):
                curr_output = self.outputList[idx]
                curr_output.generate_report()
            time.sleep(60)

    def init_input_list(self):
        new_dummy_input = DummyInputImpl()
        self.inputsList.append(new_dummy_input)



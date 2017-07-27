from model import *
import time
from pprint import pprint
from config import Config

config = Config()

class Engine:

    def __init__(self):
        self.rules = Rule().get()
        self.wasl = Wasl(config.es)

    def run(self):
        for id, label_id, wasl_query in self.rules:        
            result = list(self.wasl.scroll(wasl_query))
            print wasl_query, len(result)
             
if __name__ == '__main__':
    E = Engine()
    E.run()
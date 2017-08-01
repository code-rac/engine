from model import *
import time
import traceback
from pprint import pprint
from config import Config
import base64

config = Config()
CACHE_AGENT_ID = {}

class Engine:

    def __init__(self):
        self.rules = list(Rule().get())
        self.wasl = Wasl(config.es)
        self.agent = Agent()
        self.alert = Alert()

    def run(self):
        self.start_search = time.time() - 36000
        while 1:
            try:
                self.end_search = time.time() - 30
                self.load_agents()
                for id, label_id, wasl_query, tag in self.rules:
                    wasl_query = base64.b64decode(wasl_query)
                    logs = list(self.wasl.scroll(wasl_query, '*', time_backward(self.start_search).replace(' ', 'T'), time_backward(self.end_search).replace(' ', 'T')))
                    print len(logs), wasl_query
                    grouped_logs = self.group_by_agent(logs)
                    for agent_name in grouped_logs.keys():
                        start_at, end_at, attacker = self.get_alert_info(grouped_logs[agent_name])
                        alert = {
                            'label_id': label_id,
                            'victim_id': CACHE_AGENT_ID[agent_name],
                            'type': 'attack',
                            'false_positive': 0,
                            'attacker': attacker,
                            'start_at': start_at,
                            'end_at': end_at
                        }
                        self.alert.insert(alert)
                        pprint(alert)
                self.start_search = self.end_search
                time.sleep(0.1)
            except:
                print(traceback.format_exc())

    def load_agents(self):
        result = self.agent.get()
        CACHE_AGENT_ID.update(dict(zip([item[1] for item in result], [item[0] for item in result])))

    def group_by_agent(self, logs):
        result = {}
        for log in logs:
            if log['_type'] not in result:
                result[log['_type']] = [log['_source']]
            else:
                result[log['_type']].append(log['_source'])
        return result

    def get_alert_info(self, logs):
        start_at = '9999-99-99T99:99:99'
        end_at = '0000-00-00T00:00:00'
        attacker = set() 
        for log in logs:
            if log['arrived_time'] > end_at:
                end_at = log['arrived_time']
            if log['arrived_time'] < start_at:
                start_at = log['arrived_time'] 
            attacker.add(log['remote_host'])
        return start_at.replace('T', ' '), end_at.replace('T', ' '), ','.join(list(attacker))

if __name__ == '__main__':
    E = Engine()
    E.run()


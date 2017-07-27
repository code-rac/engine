from model import *
import time
from pprint import pprint
from config import Config

config = Config()
CACHE_AGENT_ID = {}

class Engine:

    def __init__(self):
        self.rules = list(Rule().get())
        self.wasl = Wasl(config.es)
        self.agent = Agent()
        self.alert = Alert()

    def run(self):
        self.load_agents()
        for id, label_id, wasl_query, tag in self.rules:
            logs = list(self.wasl.scroll(wasl_query))
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
                pprint(alert)
                self.alert.insert(alert)

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
        start_at = '9999-06-22T09:02:17'
        end_at = '0000-06-22T09:02:17'
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


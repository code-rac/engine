from config import Config
from elasticsearch import helpers
from pprint import pprint
import traceback
from wasl import Wasl
config = Config()


class Label:

    def __init__(self):
        pass

    def migrate(self):
        conn, cur = config.mysql_conn, config.mysql_cur
        cur.execute('''
            CREATE TABLE IF NOT EXISTS `labels` (
                id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
                name TEXT,
                description TEXT,
                severity int(11),
                reference TEXT
            )
        ''')
        conn.commit()

    def get(self):
        conn, cur = config.mysql_conn, config.mysql_cur
        cur.execute('SELECT * FROM `users`')
        return cur.fetchall()


class Alert:

    def __init__(self):
        pass

    def migrate(self):
        conn, cur = config.mysql_conn, config.mysql_cur
        cur.execute('''
            CREATE TABLE IF NOT EXISTS `alerts` (
                `id` int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
                `label_id` int(11),
                `victim_id` int(11),
                `type` TEXT,
                `false_positive` tinyint(1),
                `start_at` timestamp,
                `end_at` timestamp,
                `attacker` TEXT,
                `screenshot` TEXT,
                FOREIGN KEY (label_id) REFERENCES labels(id)
            )
        ''')
        conn.commit()


class Rule:

    def __init__(self):
        pass

    def migrate(self):
        conn, cur = config.mysql_conn, config.mysql_cur
        cur.execute('''
            CREATE TABLE IF NOT EXISTS `rules` (
                id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
                label_id int(11),
                wasl_query TEXT,
                tag TEXT,
                FOREIGN KEY (label_id) REFERENCES labels(id)
            )
        ''')
        conn.commit()

    def get(self):
        conn, cur = config.mysql_conn, config.mysql_cur
        cur.execute('SELECT * FROM `rules`')
        return cur.fetchall()


class Agent:

    def __init__(self):
        pass

    def migrate(self):
        conn, cur = config.mysql_conn, config.mysql_cur
        cur.execute('''
            CREATE TABLE IF NOT EXISTS `agents` (
                id int(11) PRIMARY KEY NOT NULL AUTO_INCREMENT,
                agent_name TEXT
            )
        ''')
        conn.commit()


class Log:

    def __init__(self):
        self.wasl = Wasl(config.es)

    def get(self, wasl_query):
        pprint(self.wasl.wasl2elasticsearch(wasl_query))
        for item in self.wasl.scroll(wasl_query):
            print(item)

def reset_database():
    conn, cur = config.mysql_conn, config.mysql_cur
    cur.execute('DROP TABLE `alerts`')
    cur.execute('DROP TABLE `rules`')
    cur.execute('DROP TABLE `labels`')
    cur.execute('DROP TABLE `agents`')
    conn.commit()


if __name__ == '__main__':
    reset_database()

    Agent().migrate()
    Label().migrate()
    Alert().migrate()
    Rule().migrate()

    # L = Log()
    # L.get('url="a"')
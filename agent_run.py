import ConfigParser
import logging
import os
import time
from multiprocessing import Process, Manager, Value

from agent import Agent
from db import DB

config = ConfigParser.ConfigParser()
config.readfp(open('config.cfg'))

logging.basicConfig(filename=config.get('Logger', 'file'), level=config.get('Logger', 'level'),
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.info('Agent Started')


def config_to_dict(config):
    conf = {}
    for i in config:
        conf[i[0]] = i[1]
    return conf


def f(job_record, queue_manager, mailer_value):
    logging.debug("Job Start - {}".format(job_record.jobid))
    queue_manager[job.jobid] = os.getpid()
    db_x = DB(config_to_dict(config.items('DB')), mail_settings=config_to_dict(config.items('Mail')),
              mailer_value=mailer_value)
    agent_new = Agent(db_x, mail_settings=config_to_dict(config.items('Mail')))
    agent_new.job_start(job_record)
    agent_new.job_step_execute()
    agent_new.job_finish()
    logging.debug("Job End - {}".format(job_record.jobid))
    queue_manager.pop(job_record.jobid)


if __name__ == '__main__':
    # print config_to_dict( config.items('DB')).get('db_user')
    manager = Manager()
    queue = manager.dict()
    mailer_value = Value('i', 0)
    db = DB(config_to_dict(config.items('DB')), mail_settings=config_to_dict(config.items('Mail')),
            mailer_value=mailer_value)
    agent = Agent(db, clear_zombies=True, mail_settings=config_to_dict(config.items('Mail')))
    while True:
        for job in agent.get_jobs():
            if not queue.has_key(job.jobid):
                p = Process(target=f, args=(job, queue, mailer_value))
                p.start()
        time.sleep(5)

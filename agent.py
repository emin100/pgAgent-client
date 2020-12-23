# -*- coding: utf-8 -*-
import logging
import os
import socket
import subprocess
import re

from db import DB
from mail import MailSender


class Agent(MailSender):
    db_connection = None
    hostname = None
    pid = None
    process_id = None
    job_id = None
    log_id = None
    status = 'i'
    message = ''
    code = 1
    extract_emails = []
    job = {}
    commands = ''
    mail_settings = []

    def __init__(self, db_connection, clear_zombies=False, mail_settings=None):
        self.db_connection = db_connection
        self.hostname = socket.gethostname()
        self.process_id = os.getpid()
        self.mail_settings = mail_settings
        if clear_zombies is True:
            self.clear_zombies()
        else:
            self.register_agent()

    def get_message(self, message):
        return str(self.process_id) + ' - ' + str(message)

    def connect_db(self):
        logging.info(self.get_message('Try to connect pgagent db'))
        try:
            self.db_connection.connect()
            logging.info(self.get_message('Connected to pgagent db'))
        except Exception as e:
            logging.error(self.get_message(e))
            # exit()

    def get_agent_id(self):
        logging.info(self.get_message('Try to get agent id'))
        self.connect_db()
        result = []
        try:
            result = self.db_connection.execute(
                "SELECT jagpid FROM pgagent.pga_jobagent WHERE jagstation = '" + self.hostname + "'", get_result=True,
                get_list=True)
        except Exception as e:
            logging.error(self.get_message('Agent not registered.'))
            logging.error(e)
        self.db_connection.close()
        return result

    def register_agent(self):
        logging.info(self.get_message('Try to register agent'))

        try:
            result = self.get_agent_id()

            if len(result) == 0:
                self.connect_db()
                self.db_connection.execute(
                    """INSERT INTO pgagent.pga_jobagent (jagpid, jagstation) 
                    VALUES(pg_backend_pid(), '{}')""".format(self.hostname))

                self.db_connection.commit()
                self.db_connection.close()
                result = self.get_agent_id()
                self.pid = result[0].jagpid
            else:
                self.pid = result[0].jagpid
            logging.info(self.get_message('Agent Registered'))
        except Exception as e:
            logging.error(self.get_message('Agent not registered.'))
            logging.error(e)
            self.db_connection.close()

    def get_jobs(self):
        logging.info(self.get_message('Try to get job list'))
        self.connect_db()
        try:
            sql = """SELECT *
                      FROM pgagent.pga_job J 
                     WHERE jobenabled 
                       AND jobagentid IS NULL 
                       AND jobnextrun <= now() 
                       AND /*(jobhostagent = '' OR */jobhostagent = '{}'/*)*/
                     ORDER BY jobnextrun""".format(self.hostname)

            list = self.db_connection.execute(sql, get_result=True, get_list=True)
            logging.info(self.get_message('Job list receive'))
            self.db_connection.close()
            return list
        except Exception as e:
            logging.error(self.get_message('Job list receive error'))
            logging.error(e)
        self.db_connection.close()

    def job_start(self, job):
        logging.info(self.get_message('Try to job start'))
        self.connect_db()
        self.job_id = job.jobid
        self.job = job
        try:
            self.extract_emails = re.findall(r'[\w\.-]+@[\w\.-]+', job.jobdesc)
            sql = """UPDATE pgagent.pga_job SET jobagentid={}, joblastrun=now()
            WHERE jobagentid IS NULL AND jobid={}""".format(self.pid, self.job_id)

            self.db_connection.execute(sql)

            sql = """SELECT nextval('pgagent.pga_joblog_jlgid_seq') AS id"""
            self.log_id = self.db_connection.execute(sql, get_result=True).id

            sql = """INSERT INTO pgagent.pga_joblog(jlgid, jlgjobid, jlgstatus) VALUES ({},{}, 'r')""" \
                .format(self.log_id, self.job_id)

            self.db_connection.execute(sql)

            self.db_connection.commit()
            logging.info(self.get_message('Job started'))
        except Exception as e:
            logging.error(self.get_message('Job start error'))
            logging.error(e)
            # self.db_connection.rollback()
            # self.db_connection.close()
            # self.job_start(id)
        self.db_connection.close()

    def job_finish(self):
        logging.info(self.get_message('Try to job finish'))
        self.connect_db()
        try:
            sql = """UPDATE pgagent.pga_joblog
            SET jlgstatus = '{}', jlgduration = now() - jlgstart
            WHERE jlgid ={}""".format(self.status, self.log_id)

            self.db_connection.execute(sql)

            sql = """UPDATE pgagent.pga_job
            SET jobagentid = NULL, jobnextrun = NULL
            WHERE jobid = {}""".format(self.job_id)
            self.db_connection.execute(sql)

            self.db_connection.commit()
            logging.info(self.get_message('Job finished'))

        except Exception as e:
            logging.error(self.get_message('Job finish error'))
            logging.error(e)

        if self.status == 'f':
            message = "\nError: \n " + self.message + "\nExecuted Commands : \n" + self.commands
            self.sendmail('Cron Problem : ' + self.job.jobname, self.extract_emails, message)
        self.db_connection.close()

    def job_step_execute(self):
        logging.info(self.get_message('Try to execute job'))

        try:
            self.connect_db()
            sql = """SELECT *  FROM pgagent.pga_jobstep 
                    WHERE jstenabled AND jstjobid={}
                    ORDER BY jstname, jstid""".format(self.job_id)

            result = self.db_connection.execute(sql, get_result=True, get_list=True)
            self.db_connection.close()
            result_status = 's'
            for step in result:

                self.commands = str(step.jstcode).strip() + "\n"
                self.extract_emails += re.findall(r'[\w\.-]+@[\w\.-]+', step.jstdesc)
                self.connect_db()
                sql = "SELECT nextval('pgagent.pga_jobsteplog_jslid_seq') AS id"
                jstlogid = self.db_connection.execute(sql, get_result=True).id
                sql = """INSERT INTO pgagent.pga_jobsteplog(jslid, jsljlgid, jsljstid, jslstatus) 
                    SELECT  {},{},{}, 'r' FROM pgagent.pga_jobstep WHERE jstid={}""".format(jstlogid, self.log_id,
                                                                                            step.jstid, step.jstid)
                self.db_connection.execute(sql)
                self.db_connection.commit()
                self.db_connection.close()
                if step.jstkind == 'b':
                    self.execute_job_b(step)
                elif step.jstkind == 's':
                    self.execute_job_s(step)
                else:
                    self.message = 'Invalid step type!'
                    self.status = 'f'
                    self.code = -1
                self.connect_db()
                sql = """UPDATE pgagent.pga_jobsteplog 
                    SET jslduration = now() - jslstart, jslresult = %s, jslstatus = %s, jsloutput = %s
                    WHERE jslid=%s"""
                self.db_connection.execute(sql, var_list=(self.code, self.status, self.message, jstlogid))
                self.db_connection.commit()
                self.db_connection.close()
                if self.status == 'f' and step.jstonerror == 'f':
                    result_status = 'f'
                    break
            self.status = result_status
        except Exception as e:
            logging.error(self.get_message('Job execute error'))
            logging.error(e)
            self.db_connection.close()
        logging.info(self.get_message('Job execute finished'))

    def execute_job_b(self, job_step):
        # self.connect_db()
        try:
            logging.debug(self.get_message('Job executed commands' + str(job_step.jstcode)))
            ex = subprocess.check_output(str(job_step.jstcode).strip(), shell=True,
                                         stderr=subprocess.STDOUT)
            logging.debug(self.get_message(ex))
            self.message = ex
            self.code = 0
            self.status = 's'

        except Exception as e:
            logging.error(e)
            self.message = e.output
            self.code = e.returncode
            self.status = job_step.jstonerror
        # self.db_connection.close()

    def execute_job_s(self, job_step):
        if job_step.jstconnstr == '':
            config = self.db_connection.config_parser
            config['db_name'] = job_step.jstdbname
            db = DB(config)

        else:
            db = DB(dsn=job_step.jstconnstr)
        try:
            logging.debug(self.get_message('Job executed Sql ' + str(job_step.jstcode)))
            db.connect()
            db.execute(job_step.jstcode)
            if len(db.connection.notices):
                notices = db.connection.notices
            else:
                notices = ''
            db.commit()
            db.close()
            self.code = 1
            self.status = 's'
            self.message = notices
            logging.debug(self.get_message(str(self.message)))
        except Exception as e:
            logging.error(e)
            if hasattr(e, 'message'):
                self.message = e.message
            else:
                self.message = e

            self.code = -1
            self.status = job_step.jstonerror
            db.close()

    def clear_zombies(self):
        logging.debug(self.get_message('Start clear zombies'))
        sql = """CREATE TEMP TABLE pga_tmp_zombies(jagpid int4);
            INSERT INTO pga_tmp_zombies (jagpid) 
            SELECT jagpid 
              FROM pgagent.pga_jobagent AG 
              --LEFT JOIN pg_stat_activity PA ON jagpid=pid 
             WHERE /*pid IS NULL AND*/ AG.jagstation = '{}';
            
            
            UPDATE pgagent.pga_joblog SET jlgstatus='d' WHERE jlgid IN (
            SELECT jlgid 
            FROM pga_tmp_zombies z, pgagent.pga_job j, pgagent.pga_joblog l 
            WHERE z.jagpid=j.jobagentid AND j.jobid = l.jlgjobid AND l.jlgstatus='r');
            
            UPDATE pgagent.pga_jobsteplog SET jslstatus='d' WHERE jslid IN ( 
            SELECT jslid 
            FROM pga_tmp_zombies z, pgagent.pga_job j, pgagent.pga_joblog l, pgagent.pga_jobsteplog s 
            WHERE z.jagpid=j.jobagentid AND j.jobid = l.jlgjobid AND l.jlgid = s.jsljlgid AND s.jslstatus='r');
            
            UPDATE pgagent.pga_job SET jobagentid=NULL, jobnextrun=NULL 
              WHERE jobagentid IN (SELECT jagpid FROM pga_tmp_zombies);
            
            DELETE FROM pgagent.pga_jobagent 
              WHERE jagpid IN (SELECT jagpid FROM pga_tmp_zombies);
            
            DROP TABLE pga_tmp_zombies;""".format(self.hostname)

        self.connect_db()
        self.db_connection.execute(sql)
        self.db_connection.commit()
        self.db_connection.close()
        self.register_agent()

        logging.debug(self.get_message('Finish clear zombies'))

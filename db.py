import logging

import psycopg2
import psycopg2.extras
import time

from mail import MailSender


class DB(MailSender):
    db_user = None
    db_pass = None
    db_name = None
    db_host = None
    db_port = None
    db_timeout = 10
    connection = None
    config_parser = None
    dsn = None
    mail_settings = []
    mailer_value = None

    def __init__(self, config_parser=None, dsn=None, mail_settings=[], mailer_value=None):
        self.mail_settings = mail_settings
        if mailer_value is not None:
            self.mailer_value = mailer_value
        if dsn is not None:
            self.dsn = dsn
        else:
            self.db_user = config_parser.get('db_user')
            self.db_pass = config_parser.get('db_pass')
            self.db_name = config_parser.get('db_name')
            self.db_host = config_parser.get('db_host')
            self.db_port = config_parser.get('db_port')
            self.db_timeout = config_parser.get('db_timeout')
            self.config_parser = config_parser
            self.dsn = None
        logging.debug('Config Parsed')

    def connect(self):
        logging.debug('Connection Start')
        try:
            if self.dsn is None:
                self.connection = psycopg2.connect(host=self.db_host, user=self.db_user, password=self.db_pass,
                                                   port=self.db_port,
                                                   database=self.db_name)
                if self.mailer_value is not None:
                    if self.mailer_value.value == 1:
                        self.mailer_value.value = 0
                        self.sendmail('DB Problem Resolved', body="Db Connection Problem Resolved")
            else:
                self.connection = psycopg2.connect(self.dsn)
            logging.debug('Connected to agent DB')
        except Exception as e:
            logging.error(e)
            if self.mailer_value is not None:
                if self.mailer_value.value == 0:
                    self.sendmail('DB Problem', body=str(e))

            if self.dsn is None:
                if self.mailer_value is not None:
                    self.mailer_value.value = 1
                time.sleep(15)
                self.connect()
            else:
                raise e

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        if self.connection.closed == 0:
            self.rollback()
            self.connection.close()

    def execute(self, query, var_list=None, get_result=False, get_list=False):
        cur = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        try:
            logging.debug(query)
            if var_list is not None:
                query = cur.mogrify(query, var_list)
                logging.debug(query)
            cur.execute(query)
            if get_result is True:

                if cur.rowcount == 1 and get_list is False:
                    result = cur.fetchone()
                else:
                    result = cur.fetchall()

                logging.debug(result)
                return result
        except Exception as e:
            self.rollback()
            logging.error(e)
            raise e
        finally:
            cur.close()

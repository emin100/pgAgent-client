# -*- coding: utf-8 -*-
import smtplib
import logging


class MailSender:
    mail_settings = []

    def __init__(self, mail_settings):
        self.mail_settings = mail_settings

    def sendmail(self, subject, receivers=[], body=None):

        try:
            if self.mail_settings['send_mail'] == 'true':
                if self.mail_settings['receiver'] != '':
                    receivers += eval(self.mail_settings['receiver'])
                tomail = ''
                for rec in receivers:
                    tomail += "<" + rec + ">"
                sender = self.mail_settings['smtp_user']
                smtp_obj = smtplib.SMTP(self.mail_settings['smtp_host'])
                smtp_obj.login(self.mail_settings['smtp_user'], self.mail_settings['smtp_password'])
                message = """From: Cron <""" + sender + """>
To: """ + tomail + """
Subject: """ + subject + """

""" + body

                smtp_obj.sendmail(sender, receivers, message)
                logging.info("Successfully sent email to (" + ",".join(receivers) + ")")
        except smtplib.SMTPException:
            logging.error("Unable to send email(" + ",".join(receivers) + ")")

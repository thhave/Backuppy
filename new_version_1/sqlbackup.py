import datetime
import os
import pyodbc as db
from arc7z import Archiver
import logging


class SQLBackup:
    driver = 'ODBC Driver 11 for SQL Server'
    server = '.'
    db_name = 'master'
    uid = ''
    pwd = ''
    con_str = ''
    con = None
    cur = None

    def __init__(self, server, db_name, uid, pwd):
        self.logger = logging.getLogger()
        self.server = server
        self.db_name = db_name
        self.uid = uid
        self.pwd = pwd
        self.con_str = r'DRIVER={{{0}}};SERVER={1};DATABASE={2};UID={3};PWD={4}; Trusted_Connection=yes' \
            .format(self.driver,
                    self.server,
                    self.db_name,
                    self.uid,
                    self.pwd)
        self.logger.info(u'SQLBackup initiated')

    def __connect__(self):
        self.con = db.connect(self.con_str, autocommit=True)
        self.cur = self.con.cursor()
        self.logger.info('Connected to server {}, database {}, user {}'.format(self.server, self.db_name, self.uid))

    def __disconnect__(self):
        self.con.close()
        self.logger.info("Connection closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.__disconnect__()
        except:
            pass

    def backup(self, path):
        self.__connect__()
        bak_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path,
                                     self.db_name + '_' + str(datetime.date.today()) + '.bak')
        try:
            if os.path.exists(bak_file_path):
                os.remove(bak_file_path)
        except Exception as e:
            self.logger.error("Error while removing old bak file: " + str(e))

        try:
            self.cur.execute('BACKUP DATABASE ? TO DISK=?', [self.db_name, bak_file_path])
            while self.cur.nextset():
                pass

        except Exception as e:
            self.logger.error('Can\'t perform a backup: ' + str(e))
        try:
            self.cur.execute('RESTORE VERIFYONLY FROM DISK =?', [bak_file_path])
            while self.cur.nextset():
                pass
            self.logger.info('BACKUP VERIFIED')
        except Exception as e:
            self.logger.error('Error while verifying the backup: ' + str(e))
        try:
            with Archiver() as arc:
                arc.compress(bak_file_path,
                             os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                          path, os.path.basename(bak_file_path) + '.7z'))
        except Exception as e:
            self.logger.error('Compressing the backup failed: ' + str(e))
        self.cur.close()
        self.__disconnect__()

import hashlib
import logging
import os
import threading
from datetime import datetime, timedelta
from ftplib import FTP
from pathlib import Path


class FTPSync:
    marker_filename_salt = 'jPgHOD8.fC0'
    marker_filename = None
    failed_path = None
    logger = None
    host = None
    port = None
    user = None
    password = None
    encoding = None
    can_rewrite = None
    ftp = None
    remote_path = None
    newer_than = None
    older_than = None

    mask = ['*.*']
    includeSubdirs = True

    # Initialization method (called by class instantiation).
    def __init__(self, host='127.0.0.1', port=21, encoding='utf-8', can_rewrite=False,
                 log_filename=u'FTPSync-events.log'):

        application_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.marker_filename = application_path + "\\" + "MARKER_" + self.marker_filename_salt
        self.failed_path = None
        self.logger = logging.getLogger()
        self.host = host
        self.port = port
        self.encoding = encoding
        self.can_rewrite = can_rewrite
        self.getoffset()
        self.ftp = FTP()
        self.logger.info(u'FTPCopy initiated')

    def connect(self, user='anonymous', password=''):
        self.user = user
        self.password = password
        try:
            self.ftp.connect(self.host, self.port)
            self.ftp.login(self.user, self.password)
        except Exception as e:
            self.logger.error('Failed to connect: ' + str(e))
            return
        else:
            self.ftp.encoding = self.encoding
            self.logger.info('Connected to FTP: ' + self.user + '@' + self.host + ':' + str(self.port) + '\n')

    def reconnect(self):
        try:
            self.connect(self.user, self.password)
        except Exception as e:
            self.logger.error('Failed to reconnect: ' + str(e))
        else:
            self.logger.info('Reconnected successfully: ' + self.user)

    def backup(self, from_paths, to_path, newer_than=3, older_than=1, create_dir=True):
        self.remote_path = to_path
        self.newer_than = datetime.now() - timedelta(days=newer_than)
        self.older_than = datetime.now() - timedelta(days=older_than)
        self.logger.debug('Backup files newer than ' + str(self.newer_than) + ' days')
        self.logger.debug('Backup files older than ' + str(self.older_than) + ' days')
        try:
            self.ftp.cwd(self.remote_path)
        except Exception as e:
            self.logger.error('Failed to change remote directory: ' + self.remote_path + ' ' + str(e))
        else:
            for path in from_paths:
                try:
                    self.__upload__(os.path.normpath(path), create_dir)
                except Exception as e:
                    self.logger.error('Error uploading ' + str(os.path.normpath(path)) + ' ' + str(e))
            self.logger.info('Backup has been made. Check for errors above')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.disconnect()
        except:
            pass

    def __compare__(self, local_path, remote_path, by_size=True):
        self.ftp.sendcmd('TYPE i')
        self.logger.info('Comparing: %s', local_path)
        if by_size and (self.ftp.size(remote_path) is not None):
            remote_file_size = self.ftp.size(remote_path)
            local_file_size = os.path.getsize(local_path)
            self.logger.info('...by size: ' + str(remote_file_size) + ' ' + str(local_file_size))
            if remote_file_size == local_file_size:
                self.logger.info('...sizes are equals')
                return True
            else:
                self.logger.info('...sizes are different')
                return False
        else:  # by hash
            with open(local_path, 'rb') as local_file:
                local_file_hash = hashlib.md5(local_file.read()).hexdigest()
                m = hashlib.md5()
                self.ftp.retrbinary('RETR %s' % remote_path, m.update)
                remote_file_hash = m.hexdigest()
                self.logger.info('...by hash: ' + str(remote_file_hash) + ' ' + str(local_file_hash))
                if local_file_hash == remote_file_hash:
                    self.logger.info('...files are equals')
                    return True
                else:
                    self.logger.info('...files are different')
                    return False

    def checkoffset(self, local_path, remote_path):
        local_path = os.path.normpath(local_path)
        if (Path(local_path).is_file()) and (self.failed_path == str(local_path)):
            try:
                size = self.ftp.size(remote_path)
                return size
            except Exception as e:
                self.logger.error('Cannot calculate offset: %s', str(e))
                return None
        else:
            return None

    def getoffset(self):
        if Path(self.marker_filename).is_file():
            with open(self.marker_filename, 'rb') as file:
                try:
                    data = file.read()
                    data = data.decode('utf-8')
                    self.logger.debug('getLastFailedUploadOffset data: %s', data)
                    self.failed_path = data
                except Exception as e:
                    self.logger.error('Error in getLastFailedUploadOffset: %s', str(e))
                    self.failed_path = None
        else:
            self.failed_path = None

    def setoffset(self, path):
        path = os.path.normpath(path)
        with open(self.marker_filename, 'wb') as file:
            try:
                file.write((str(path)).encode('utf-8'))
                file.flush()
            except Exception as e:
                self.logger.critical("Can't write marker file! %s", str(e))

    def __store__(self, local_path, remote_path=r'/', block_size=8192, noop_time=120):
        self.logger.info('Storing a file: ' + str(local_path))
        self.ftp.voidcmd('TYPE I')
        try:
            local_file = open(local_path, 'rb')
        except Exception as e:
            self.logger.error('Failed to open local file: %s; %s', local_path, str(e))
            return
        try:
            offset = self.checkoffset(local_path, remote_path)
            if offset is None:
                connection = self.ftp.transfercmd('STOR ' + remote_path)
            else:
                try:
                    connection = self.ftp.transfercmd('STOR ' + remote_path, rest=str(offset))
                except Exception as e:
                    self.logger.warning("Can't resume last failed upload: " + str(e))
                    connection = self.ftp.transfercmd('STOR ' + remote_path)

        except Exception as e:
            self.logger.error('Failed to store file on FTP: %s; %s', remote_path, str(e))
            return
        finally:
            self.setoffset(local_path)

        def background():
            if offset is not None:
                local_file.seek(offset)
            while True:
                block = local_file.read(block_size)
                if not block:
                    break
                try:
                    connection.sendall(block)
                except Exception as e:
                    self.logger.error('...failed to send a block: ' + str(e))
                    break
            connection.close()
            try:
                self.setoffset(' ')
                os.remove(self.marker_filename)
            except:
                pass

        t = threading.Thread(target=background)
        t.start()
        while t.is_alive():
            t.join(noop_time)
            alive_cmd = 'NOOP'  # 'PWD' 'SYST'
            self.ftp.voidcmd(alive_cmd)
        return self.ftp.voidresp()

    def __upload__(self, local_path, create_dir=True):
        if create_dir:
            current_remote_path = os.path.basename(local_path)
        try:
            ftp_path_list = self.ftp.nlst()
        except Exception as e:
            self.logger.error('Failed to list files on FTP: %s; %s', local_path, str(e))
            return

        if Path(local_path).is_file():
            for i in range(0, len(self.mask)):
                if not Path(local_path).match(self.mask[i]):  # If the file doesn't match the mask
                    return
            self.logger.info('Uploading a file: ' + str(local_path))
            local_path_time = datetime.fromtimestamp(os.path.getmtime(local_path))
            if (local_path_time < self.newer_than) or (local_path_time > self.older_than):
                return
            condition_of_copying = ((current_remote_path not in ftp_path_list) or self.can_rewrite or
                                    ((current_remote_path in ftp_path_list) and
                                     (not self.__compare__(local_path, current_remote_path))))
            if condition_of_copying:
                try:
                    self.__store__(local_path, current_remote_path)
                except Exception as e:
                    self.logger.error('...failed to store the file: ' + str(local_path) + ' ' + str(e))
                else:
                    self.logger.info('...successfully stored file: ' + str(local_path))

        elif Path(local_path).is_dir():
            self.logger.info('Uploading a directory: ' + str(local_path))
            try:
                os.chdir(local_path)
            except Exception as e:
                self.logger.error('...changing local dir failed: %s', str(e))
            else:
                if create_dir:
                    if (current_remote_path not in ftp_path_list) and (current_remote_path != ''):
                        try:
                            self.ftp.mkd(current_remote_path)
                        except Exception as e:
                            self.logger.error('...creating remote dir failed: %s', str(e))
                        else:
                            self.logger.info('...created remote dir: %s', str(current_remote_path))
                    self.ftp.cwd(current_remote_path)
                for path in os.listdir(local_path):
                    try:
                        self.__upload__(os.path.join(os.getcwd(), path))
                    except Exception as e:
                        self.logger.error('...failed to upload inner files: %s; %s', local_path, str(e))
                self.ftp.cwd('..')
                os.chdir('..')

    def disconnect(self):
        try:
            self.ftp.quit()
        except:
            pass
        else:
            self.logger.info('Disconnected.')

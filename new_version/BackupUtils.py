from ftplib import FTP, error_reply
from pathlib import Path
from datetime import datetime, timedelta
import ntpath, os, hashlib, threading, logging, sys

class FTPSync:
    
    __version__ = '0.1'

    markerFileNameSalt = 'TWFya2Vy.RlRQQ29weQ'
    markerFileName = None
    failedPath = None
    logger = None
    host = None
    port = None
    user = None
    password = None
    encoding = None
    canRewrite = None
    ftp = None
    remotePath = None
    datePass = None
    datePassBelow = None


    
    # Initialization method (called by class instantiation).
    def __init__(self, host = '127.0.0.1', port = 21, encoding = 'utf-8', canRewrite = False, logFileName=u'FTPSync-events.log'):

        #FOR PyInstaller OneFile Mode only
##        if getattr(sys, 'frozen', False):
##            # If the application is run as a bundle, the pyInstaller bootloader
##            # extends the sys module by a flag frozen=True and sets the app 
##            # path into variable _MEIPASS'.
##            application_path = sys._MEIPASS
##        else:
##            application_path = os.path.dirname(os.path.abspath(__file__))
        #For PyInstaller OneFolder mode only (or for using without creating standalone executable)
        application_path = os.path.dirname(os.path.abspath(__file__))
        self.markerFileName = application_path + "\\" + self.markerFileNameSalt
        self.failedPath = None
        self.logger = logging.getLogger()
        logging.basicConfig(level=logging.DEBUG, filename = logFileName, format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
        self.host = host
        self.port = port
        self.encoding = encoding
        self.canRewrite = canRewrite
        self.getLastFailedUploadOffset()
        self.ftp = FTP()
        self.logger.info(u'------FTPCopy %s initiated------', self.__version__)
      
        
    def connectUser(self, user = 'anonymous', password = ''):
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
            self.connectUser(user, password)
        except Exception as e:
            self.logger.error('Failed to reconnect: ' + str(e))
        else:
            self.logger.info('Reconnected successfully: ' + user)
            

    def backup(self, fromPaths, toPath, datePass = 3, datePassBelow = 1):
        self.remotePath = toPath
        self.datePass = datetime.now() - timedelta(days = datePass)
        self.datePassBelow = datetime.now() - timedelta(days = datePassBelow)
        self.logger.debug('datePass = ' + str(self.datePass))
        self.logger.debug('datePassBelow = ' + str(self.datePassBelow))
        try:
            self.ftp.cwd(self.remotePath)
        except Exception as e:
            self.logger.error('Failed to change remote directory: ' + self.remotePath + ' ' + str(e))
        else:
            for path in fromPaths:
                try:
                    self.__upload__(os.path.normpath(path))
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
        
    def __compare__(self, localPath, remotePath, bySize = True):
        self.ftp.sendcmd('TYPE i')
        self.logger.info('Comparing: %s', localPath)
        if bySize and (self.ftp.size(remotePath) != None):
            remoteFileSize = self.ftp.size(remotePath)
            localFileSize = os.path.getsize(localPath)
            self.logger.info('...by size: ' +  str(remoteFileSize) + ' ' + str(localFileSize))
            if remoteFileSize == localFileSize:
                self.logger.info('...sizes are equals')
                return True
            else:
                self.logger.info('...sizes are different')
                return False
        else: #by hash
            with open(localPath, 'rb') as localFile:
                localFileHash = hashlib.md5(localFile.read()).hexdigest()
                m = hashlib.md5()
                self.ftp.retrbinary('RETR %s' % remotePath, m.update)
                remoteFileHash = m.hexdigest()
                self.logger.info('...by hash: ' + str(remoteFileHash) + ' ' + str(localFileHash))
                if localFileHash == remoteFileHash:
                    self.logger.info('...files are equals')
                    return True
                else:
                    self.logger.info('...files are different')
                    return False

    def checkLastFailedUploadOffset(self, localPath, remotePath):
        localPath = os.path.normpath(localPath)
        if (Path(localPath).is_file()) and (self.failedPath == str(localPath)):
            try:
                size =  self.ftp.size(remotePath)
                return size
            except Exception as e:
                self.logger.error('Cannot calculate offset: %s', str(e))
                return None
        else:
            return None
            
            
    def getLastFailedUploadOffset(self):
        if (Path(self.markerFileName).is_file()):
            with open(self.markerFileName, 'rb') as file:
                try:
                    data = file.read()
                    data = data.decode('utf-8')
                    self.logger.debug('getLastFailedUploadOffset data: %s', data)
                    self.failedPath = data
                except Exception as e:
                    self.logger.error('Error in getLastFailedUploadOffset: %s', str(e))
                    self.failedPath = None
        else:
            self.failedPath = None
 
    def setOffset(self, path):
        path = os.path.normpath(path)
        with open(self.markerFileName, 'wb') as file:
            try:
                file.write((str(path)).encode('utf-8'))
                file.flush()
            except Exception as e:
                self.logger.critical("Can't write marker file! %s", str(e))
    
    def __store__(self, localPath, remotePath = r'/', block_size = 8192, noop_time = 120):
        self.logger.info('Storing a file: ' + str(localPath))
        self.ftp.voidcmd('TYPE I')
        try:
            localFile = open(localPath, 'rb')
        except Exception as e:
            self.logger.error('Failed to open local file: %s; %s', localPath, str(e))
            return
        try:
            offset = self.checkLastFailedUploadOffset(localPath, remotePath) 
            if  offset == None:
                connection = self.ftp.transfercmd('STOR ' + remotePath)
            else:
                try:
                    connection = self.ftp.transfercmd('STOR ' + remotePath, rest = str(offset)) 
                except:
                    self.logger.warning("Can't resume last failed upload!")
                    connection = self.ftp.transfercmd('STOR ' + remotePath)  
                    
        except Exception as e:
            self.logger.error('Failed to store file on FTP: %s; %s', remotePath, str(e))
            return
        finally:
            self.setOffset(localPath)
                
        def background():
            if offset != None:
                localFile.seek(offset)  
            while True:
                block = localFile.read(block_size)
                if not block:
                    break
                try:
                    connection.sendall(block)
                except:
                    self.logger.error('...failed to send a block')
                    break
            connection.close()
            try:
                    self.setOffset(' ')
                    os.remove(self.markerFileName)
            except:
                    pass
            
        
        t = threading.Thread(target = background)
        t.start()
        while t.is_alive():
            t.join(noop_time)
            alive_cmd = 'NOOP' #'PWD' 'SYST'
            self.ftp.voidcmd(alive_cmd)
        return self.ftp.voidresp()
      
    def __upload__(self, localPath):

        
        currentRemotePath = os.path.basename(localPath)
        try:
            ftpPathList = self.ftp.nlst()
        except Exception as e:
            self.logger.error('Failed to list files on FTP: %s; %s', localPath, str(e))
            return
        
        if Path(localPath).is_file():
            self.logger.info('Uploading a file: ' + str(localPath))
            localPath_time = datetime.fromtimestamp(os.path.getmtime(localPath))
            if (localPath_time < self.datePass) or (localPath_time > self.datePassBelow):
                return
            conditionOfCopying = ((not currentRemotePath in ftpPathList) or self.canRewrite or 
                                 ((currentRemotePath in ftpPathList) and 
                                  (not self.__compare__(localPath, currentRemotePath))))
            if conditionOfCopying:
                    try:
                        self.__store__(localPath, currentRemotePath)
                    except Exception as e:
                        self.logger.error('...failed to store the file: ' + str(localPath) + ' ' + str(e))
                    else:
                        self.logger.info('...successfully stored file: ' + str(localPath))
                  
        elif Path(localPath).is_dir():
            self.logger.info('Uploading a directory: ' + str(localPath))
            try:
                os.chdir(localPath)
            except Exception as e:
                self.logger.error('...changing local dir failed: %s', str(e))
            else:
                if (not currentRemotePath in ftpPathList) and (currentRemotePath != ''):
                    try:
                        self.ftp.mkd(currentRemotePath)
                    except Exception as e:
                        self.logger.error('...creating remote dir failed: %s', str(e))
                    else:
                        self.logger.info('...created remote dir: %s', str(currentRemotePath))
                self.ftp.cwd(currentRemotePath)
                for path in os.listdir(localPath):
                    try:
                        self.__upload__(os.path.join(os.getcwd(), path))
                    except Exception as e:
                        self.logger.error('...failed to upload inner files: %s; %s', localPath, str(e))                        
                self.ftp.cwd('..')
                os.chdir('..')
             
    def disconnect(self):
        try:
            self.ftp.quit()
        except:
            pass
        else:
            self.logger.info('------Disconnected.------')


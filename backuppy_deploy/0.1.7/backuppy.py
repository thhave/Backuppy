from ftplib import FTP
import ntpath
from pathlib import Path
import os
from datetime import datetime
from datetime import timedelta
from configparser import ConfigParser
import hashlib
import threading

version = '0.1.7'
#changelog 0.1.2 - Added CMP; 0.1.3 - added multiple paths; 0.1.4 - Bug fix
#0.1.5 : added special extension *.backuppy for files in progress
#0.1.6: special extension - bad idea. It requires DELETE permissions and it doesn't work with existing files.
#0.1.7 Fixed problem with timing out when downloading large files


def get_ftp_md5(ftp, remote_path):
    #Get a hash of remote file located on FTP
    #It's slow operation for large files
    logfile.write('Starting calculating a hash of remote file: ' + remote_path + ' \n')
    m = hashlib.md5()
    ftp.retrbinary('RETR %s' % remote_path, m.update)
    return m.hexdigest()

def cmpwftp(ftp, localpath, ftppath):
    #compare local file with remote file
    #if True -> files are identical
    #firstly check it by sizes
    ftp.sendcmd("TYPE i") #Binary mode 
    if not ftp.size(ftppath) == None:  
        ftpfilesize = ftp.size(ftppath)
        localfilesize = os.path.getsize(localpath)
        logfile.write('CMP Files with same names by size: ' + localpath + '\n')
        logfile.write('   Size 1: ' + str(localfilesize) + '\n')
        logfile.write('   Size 2: ' + str(ftpfilesize) + '\n')
        logfile.flush()
        if ftpfilesize == localfilesize:
            return True
        else:
            return False
    #check it by hash if size didn't got
    #for large files it takes a lot of time
    else:
        localfilehash = hashlib.md5(open(localpath, 'rb').read()).hexdigest()
        remotefilehash = get_ftp_md5(ftp, ftppath)
        logfile.write('CMP Files with same names by hash: ' + localpath + '\n')
        logfile.write('   HASH1: ' + str(localfilehash) + '\n')
        logfile.write('   HASH2: ' + str(remotefilehash) + '\n')
        if localfilehash == remotefilehash:
            return True
        else:
            return False
    
def storwithnoop(ftp, path, file, block_size = 8192):
    ftp.voidcmd('TYPE I')
    connection = ftp.transfercmd('STOR ' + path)
    def background():
        while True:
            block = file.read(block_size)
            if not block:
                break
            connection.sendall(block)
        connection.close()
    t = threading.Thread(target=background)
    t.start()
    while t.is_alive():
        t.join(60)
        ftp.voidcmd('NOOP')
    return ftp.voidresp()
def upload(path, rewrite = False, old = 3):
    #The "core" function of the script:
    #It uploads files and folders to ftp
    #old parameter defines how old files should be copied (in days)
    #For example, if it's set to 3, files older 3 days from now won't be copied
    path = os.path.normpath(path) #make path "normal"
    if Path(path).is_file():
        #copy only files newer than %old% days (from now)
        if datetime.fromtimestamp(os.path.getmtime(path)) >= (datetime.now() - timedelta(days = old)):
            #We'll copy files that doesn't exist on ftp, and if they exist, we'll copy it only if %rewrite% is true or files on ftp and local drive are different
            if (not ntpath.basename(path) in ftp.nlst()) or rewrite or ((ntpath.basename(path) in ftp.nlst()) and not (cmpwftp(ftp, path, ntpath.basename(path)))):
                logfile.write('File copying started at ' + str(datetime.now()) + ': ' + path + '\n')
                logfile.flush() 
                try:
                    f =  open(path, 'rb')
                except WindowsError as e:
                    print(e)
                    logfile.write('Open file failed: ' + str(e) + '\n')
                else:
                    try:
                        #0.1.5 added special extension for files in progress: *.backuppy
                        #0.1.6 special extension - bad idea
                        #ftp.storbinary('STOR ' + ntpath.basename(path) + '.backuppy', f)
                        #------- 0.1.7
                        #ftp.storbinary('STOR ' + ntpath.basename(path), f) #it stores a file on the ftp
                        
                       
                        storwithnoop(ftp, ntpath.basename(path),  f)
    
                            
                    except Exception as e1:
                        logfile.write('FTP Error: ' + str(e1) + '\n')
                    else:
                        #delete special extension cuz file have been copied 
                        #ftp.rename(ntpath.basename(path) + '.backuppy', ntpath.basename(path))
                        logfile.write('Copied file: '  + str(datetime.now()) + ' ' + path + '\n')
                    f.close()
            else:
                logfile.write('File exists and identical to local one: ' + path + '\n')
       
    elif Path(path).is_dir():
        try:
            os.chdir(path) #change local directory
        except WindowsError as e:
                    print(e)
                    logfile.write('Change dir failed '  + str(datetime.now()) + ': ' + str(e) + '\n')
        except Exception as e:
                    print(e)
                    logfile.write('Failed: ' + str(e) + '\n')
        else:
            if (not os.path.basename(path) in ftp.nlst()) and (os.path.basename(path) != ''):
                #make dir if it doesn't exist on the ftp
                try:
                    ftp.mkd(os.path.basename(path))
                except:
                    logfile.write("Folder can't be created " + os.path.basename(path) + '\n')
                    return
                else:
                    logfile.write('Folder created: ' + os.path.basename(path) + '\n')
            ftp.cwd(os.path.basename(path))
            logfile.write('CWD ' + str(os.path.basename(path))+ '\n')
            for file in os.listdir(path):
                #upload each file in current directory on the ftp
                upload(os.path.join(os.getcwd(), file), rewrite, old)
            #come back to previous folder
            ftp.cwd('..')
            os.chdir('..')
    logfile.flush()

#We take configuration from an ini file placed with the script
config = ConfigParser()
config.read('server.ini')
host = config['ftp']['host']
port = int(config['ftp']['port'])
user = config['ftp']['user']
password = config['ftp']['password']
ftppath = config['ftp']['ftppath']
path = config['ftp']['path']
#we can backup different paths
paths = path.split('|')
for i in range(0, len(paths)):
    paths[i] = paths[i].strip()

#setting for "bypass" files
old_setting = int(config['settings']['old'])

logfile = open('log.txt', 'a')
logfile.write('\nScript ver. ' + str(version) + '\n')
logfile.write('\nScript started at ' + str(datetime.now()) + '\n')

os.system("title "+ ' '  + str(version) + 'Резервное копирование: НЕ ЗАКРЫВАЙТЕ ЭТО ОКНО!')
print('Версия программы: ' + version)
print('Выполняется резервное копирование. НЕ ЗАКРЫВАЙТЕ ЭТО ОКНО!')
print('Копирование начато: ' + str(datetime.now()))

try:
    ftp = FTP()
    ftp.connect(host, port)
    logfile.write('Connected to ' + host + ':' + str(port) + '\n')
    ftp.login(user, password)
    logfile.write('Logged ftp by "' + user + '"\n')
    ftp.encoding='utf-8'
    ftp.cwd(ftppath)
    logfile.write('CWD ' + str(ftppath) + '\n')
    logfile.write('Find paths: ' + str(paths) + '\n')
    logfile.flush()
    for p in paths:
        try:
            upload(p, rewrite = False, old = old_setting)
        except Exception as e:
            print(str(e))
            
except Exception as e:
    logfile.write('General exception: ' + str(e) + '\n')
else:
    logfile.write('Script has been done the job successfully' + '\n')
    print('Успешно!')
finally:
    logfile.write('Script exited at ' + str(datetime.now()) + '\n')
    logfile.close()
    try:
        ftp.quit()
    except:
        pass

        

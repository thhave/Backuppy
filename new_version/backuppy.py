from BackupUtils import FTPSync
from configparser import ConfigParser
import base64, pathlib

__version__ = '0.1.8'
configFileName = 'server.ini'
saltLabel = r'JCReJl5ASEpHfn5KNzZfKz0tV0pLd3Mxa2ohQGg'
pathSplitter = '|'

def set_password(raw_password):
    return saltLabel + base64.b64encode(str.encode(raw_password)).decode()

def get_password(enc_password):
    if enc_password[0:len(saltLabel)] == saltLabel:
        return base64.b64decode(str.encode(enc_password[len(saltLabel):])).decode()
    else:
        return enc_password
print('Выполняется резервное копирование. Не закрывайте это окно')   
config = ConfigParser()
config.read(configFileName)
host = config['ftp']['host']
port = int(config['ftp']['port'])
remotePath = config['ftp']['ftppath']
    
localPaths = (config['ftp']['path']).split(pathSplitter)
for i in range(0, len(localPaths)):
    localPaths[i] = localPaths[i].strip()

user = config['ftp']['user']
read_password = config['ftp']['password']

if read_password[0:len(saltLabel)] != saltLabel:
    password = read_password
    config.set('ftp', 'password', set_password(read_password))
    with open(configFileName, 'w') as configFile:
        config.write(configFile)
else:
    password = get_password(read_password)

datePass = int(config['settings']['old'])
datePassBelow = int(config['settings']['new'])

with FTPSync(host, port) as sync:
    sync.connectUser(user, password)
    sync.backup(localPaths, remotePath, datePass, datePassBelow)


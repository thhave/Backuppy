from BackupUtils import FTPSync
from configparser import ConfigParser
from sqlbackup import SQLBackup
import base64
import os
import logging


def set_password(raw_password):
    return salt_label + str(base64.b64encode(os.urandom(16))[:-2], 'utf-8') \
           + base64.b64encode(str.encode(raw_password)).decode()


def get_password(enc_password):
    if enc_password[0:len(salt_label)] == salt_label:
        return base64.b64decode(str.encode(enc_password[len(salt_label) + 22:])).decode()
    else:
        return enc_password


__version__ = '0.1.9'
config_file_name = 'server.ini'
salt_label = base64.b64decode('UHpSNmk0QkZvcVNHRnJIb044OU5kRFo=').decode()
path_splitter = '|'
temp_folder = 'temp'

print('Backuppy. Версия {}. Выполняется резервное копирование. Не закрывайте это окно'.format(__version__))
logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG, filename="Backuppy-events.log",
                    format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
logger.info("\n\n\nBackuppy ver {} initiated".format(__version__))
config = ConfigParser()
config.read(config_file_name)

host = config['ftp']['host']  # FTP Server Host name
port = int(config['ftp']['port'])  # FTP Server Port
remote_path = config['ftp']['ftppath']  # Path on ftp server
user = config['ftp']['user']
read_password = config['ftp']['password']

task_file_list = (config['config']['tasks']).split(path_splitter)  # Task file names
logger.info("Task files: " + str(task_file_list))

if read_password[0:len(salt_label)] != salt_label:
    password = read_password
    config.set('ftp', 'password', set_password(read_password))
    with open(config_file_name, 'w') as configFile:
        config.write(configFile)
else:
    password = get_password(read_password)

for task_index in range(0, len(task_file_list)):
    taskConfig = ConfigParser()
    try:
        taskConfig.read(task_file_list[task_index])
    except Exception as e:
        logger.warning("Task file {} skipped because of error: {}".format(task_file_list[task_index], str(e)))
        continue
    if taskConfig.has_section('folder'):  # If should backup local directories
        local_paths = taskConfig['folder']['path'].split(path_splitter)
        mask = taskConfig['folder']['mask'].split(path_splitter)  # Mask for files to backup
        includeSubdirs = taskConfig['folder']['subdirs']  # Should backup subdirectories
        datePass = int(taskConfig['folder']['old'])
        datePassBelow = int(taskConfig['folder']['new'])

        for path_index in range(0, len(local_paths)):
            local_paths[path_index] = local_paths[path_index].strip()

        with FTPSync(host, port) as sync:
            sync.mask = mask  # list of file masks
            sync.includeSubdirs = includeSubdirs  # bool
            sync.connect(user, password)
            sync.backup(local_paths, remote_path, datePass, datePassBelow)

    if taskConfig.has_section('sql'):  # Should backup an mssql database
        sqlDriver = taskConfig['sql']['driver']  # MSSQL Server driver string
        server = taskConfig['sql']['server']
        dbName = taskConfig['sql']['dbName']
        uid = taskConfig['sql']['uid']
        read_pwd = taskConfig['sql']['pwd']

        if read_pwd[0:len(salt_label)] != salt_label:
            pwd = read_pwd
            taskConfig.set('sql', 'pwd', set_password(pwd))
            os.chdir(os.path.dirname(os.path.abspath(__file__)))
            with open(task_file_list[task_index], 'w') as task_config_file:
                taskConfig.write(task_config_file)
        else:
            pwd = get_password(read_pwd)

        compressSqlBak = taskConfig['sql']['compress']

        with SQLBackup(server, dbName, uid, pwd) as SQL:
            SQL.backup(temp_folder)

        with FTPSync(host, port) as sync:
            sync.mask = ['*.7z']
            sync.includeSubdirs = False
            sync.connect(user, password)
            temp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), temp_folder)
            sync.backup([temp_path], remote_path, 1, 0, create_dir=False)

        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        for the_file in \
                os.listdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), temp_folder)):
            #  Clean TEMP directory
            file_path = os.path.join(temp_folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                # elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                logger.error("Error while cleaning TEMP directory: " + str(e))

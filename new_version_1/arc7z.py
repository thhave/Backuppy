import subprocess
import os


class Archiver:
    exe_path = '7z\\7za.exe'

    def __init__(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.exe_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), self.exe_path)

    def compress(self, src, dest):
        cmd = r'"{}" a -t7z -m0=lzma2 -mx=9 -mfb=64 -md=32m -ms=on "{}" "{}"'.format(self.exe_path, dest, src)
        subprocess.call(cmd, shell=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

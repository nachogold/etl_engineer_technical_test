import sys
import subprocess
import pkg_resources

def check_requirements():
    required = {'fastparquet', 'pyarrow'}
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = required - installed

    if missing:
        print('Installing required dependencies.\n')
        python = sys.executable
        subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL) #hide output to user
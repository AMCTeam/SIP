import logging
import os
import functools
import datetime
import schedule
FILE = '/etc/logs/cops.logs'

def sys_check():
    start = str(datetime.datetime.now())
    log = str('System Check :' + ' |StartTime :' + start)
    with open(FILE, 'a') as fi:
        fi.write(log)
        fi.write('\n')

class LoggerClass(object):
    
    def __init__(self):
        schedule.every().day.at("10:00").do(sys_check)
        pass
    # def create_logger(self, *args,**kwargs):
    def create_logger(self):
        """
        Creates a logging object and returns it
        """
        
        day = datetime.datetime.now().strftime("%A")
        today = datetime.date.today()
        last_monday = today - datetime.timedelta(days=today.weekday())
        if day == "Monday":
           last_monday = today
        last_monday = last_monday.strftime("%Y-%m-%d")

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
     
        # create the error logging file handler
        err_log_path = os.path.dirname(__file__) + '/logs/errors-'+last_monday+'.log'
        eh = logging.FileHandler(err_log_path)

        # create the diagnostic logging file handler
        diag_log_path = os.path.dirname(__file__) + '/logs/diagnostics-'+last_monday+'.log'
        dh = logging.FileHandler(diag_log_path)
     
        fmt = '%(asctime)s $#$ %(name)s $#$ %(levelname)s $#$ %(message)s'
        formatter = logging.Formatter(fmt)
        eh.setFormatter(formatter)
        eh.setLevel(logging.ERROR)

        dh.setFormatter(formatter)
        dh.setLevel(logging.DEBUG)
        self.logger.handlers = []
        # add handler to logger object
        self.logger.addHandler(eh)
        self.logger.addHandler(dh)
        
    # def __call__(self):
        # return logger
        

    def log_er(self,fn):
        self.logger.debug("Debugging")
        return fn

    def with_sys_logging(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = str(datetime.datetime.now())
            result = func(*args, **kwargs)
            end = str(datetime.datetime.now())
            # return result, datetime.datetime.now() - start
            log = str('FunctionName :' + str(func.__name__) + ' |StartTime :' + start + ', ' + '|EndTime :' + end)
            with open(FILE, 'a') as fi:
                fi.write(log)
                fi.write('\n')
            return result

        return wrapper


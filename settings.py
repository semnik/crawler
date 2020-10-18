import datetime
import json
import logging
import os
import sys

import pytz as pytz

CONFIG_FILENAME = 'config.json'





try:
    with open(CONFIG_FILENAME, 'r', encoding='utf-8') as f:
        CONFIG_JSON = json.load(f)
except Exception as exc:
    print(f'error in opening json config file - {CONFIG_FILENAME}')
    sys.exit('JSON_CONFIG ERROR')

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))





LOG_DIR = os.path.join(PROJECT_DIR, 'log')
DEBUG_DIR = os.path.join(PROJECT_DIR, 'debug')
TIMEZONE = pytz.timezone('Europe/Moscow')


str_now_time = datetime.datetime.now(TIMEZONE).strftime('%Y_%m_%d_%H_%M')

log_file_path = os.path.join(LOG_DIR, str_now_time + 'crawler.log')


THREAD_WORKERS_AMOUNT = CONFIG_JSON['THREAD_WORKERS_AMOUNT']
START_URL = CONFIG_JSON['START_URL']
os.makedirs(LOG_DIR, exist_ok=True)

os.makedirs(DEBUG_DIR, exist_ok=True)


def get_logger(name=__file__, file=log_file_path, encoding='utf-8'):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '[%(asctime)s] %(filename)s:%(lineno)d %(levelname)-8s %(message)s')

    fh = logging.FileHandler(file, encoding=encoding)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(formatter)
    log.addHandler(sh)

    return log


log = get_logger()
print = log.debug

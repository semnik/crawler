import itertools
import traceback
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin

import requests
import sqlalchemy
from bs4 import BeautifulSoup

from model import Url, db_session, init_db_session, db_session_list, \
    update_parsed_flag, batch_iter
from model.url import add_url
from settings import START_URL, THREAD_WORKERS_AMOUNT, log
from settings import print
from get_url_data import get_all_website_links


def threads_worker(sqlalchemy_objects_list):
    for current_object in sqlalchemy_objects_list:
        try:
            current_object_url = current_object.url
        except Exception as exc:
            log.error(f'{exc}')

        try:
            links = get_all_website_links(current_object_url)
            update_parsed_flag(sqlalchemy_object=current_object,
                               session=db_session)

            log.debug(list(links))

            for link in links:
                add_url(url = link, db_session=db_session)
        except Exception as exc:

            log.error(traceback.format_exc())


if __name__ == "__main__":
    POSTGRES_ROWS_BATCH_SIZE = 100
    CRAWLER_VERSION = "0.9"

    log.debug(f'crawler version - {CRAWLER_VERSION}')
    log.debug(f'THREAD_WORKERS_AMOUNT - {THREAD_WORKERS_AMOUNT}')

    init_db_session()

    add_url(url=START_URL, db_session=db_session)

    while True:
        ad_records_iterable = Url.yield_not_parsed_urls(
            session=db_session_list)
        log.debug('start iteration')

        with ThreadPoolExecutor(max_workers=THREAD_WORKERS_AMOUNT) as pool:
            [pool.submit(threads_worker, sqlalchemy_urls_list)
             for sqlalchemy_urls_list in
             batch_iter(int(POSTGRES_ROWS_BATCH_SIZE /
                            THREAD_WORKERS_AMOUNT),
                        ad_records_iterable)]

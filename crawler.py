# Задача: написать супер-минималистичный краулер
# #
# # Мы запускаем скрипт, даём ему начальный URL, говорим в сколько потоков нужно краулить.
# #
# # После того, как мы остановили скрипт, мы можем из него получить список адресов,
# которые он посетил (возможно, для этого нужно запустить какой-то скрипт, который нам эти адреса выдаст).
# #
# # Содержимое страниц сохранять не нужно. Для извлечения адресов из страниц можно использовать любые средства,
# хоть регулярки (конечно, более уместные средства приветствуются).
# #
# # Потенциально мы можем захотеть запускать потоки краулера на другой машине (не нужно это реализовывать,
# но нужно описать, что для этого осталось доделать в решении).
# #
# # В решении нас в первую очередь интересует минимальная работоспособность,
# устройство проекта и культура кода.


# Я буду считать что защиты от парсинга нет

import itertools
import traceback
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, urljoin

import requests
import sqlalchemy
from bs4 import BeautifulSoup
from sqlalchemy.orm import scoped_session
from settings import print

from model import Url, db_session, init_db_session, db_session_list
from settings import START_URL, THREAD_WORKERS_AMOUNT


def get_request_data(url: str):
    '''

    :param url:
    :return: request_data
    '''

    with requests.Session() as session:
        request_data = session.get(url, timeout=50,
                                   allow_redirects=True)

    return request_data


def update_parsed_flag(sqlalchemy_object, session: scoped_session):
    '''
    updates url flag

    :param sqlalchemy_object: запись в БД в виде sqlalchemy object
    :return:
    '''

    try:
        sqlalchemy_object = session.merge(sqlalchemy_object, load=False)

        sqlalchemy_object.is_parsed = True

        try:

            session.commit()

        except Exception as exc:

            session.rollback()
            print(traceback.format_exc())

    except Exception as exc:
        print(" error - {}".format(exc))


def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def batch_iter(n, iterable):
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, n))
        if not batch:
            return
        yield batch


def threads_worker(sqlalchemy_urls_list):
    for url_sqlalchemy_object in sqlalchemy_urls_list:
        try:
            url_1 = url_sqlalchemy_object.url
        except Exception as exc:
            raise exc

        if is_valid(url_1):

            try:
                links = get_all_website_links(url_1)
                update_parsed_flag(sqlalchemy_object=url_sqlalchemy_object,
                                   session=db_session)

                print(list(links))

                for link in links:
                    add_url(link)
            except Exception as exc:
                raise exc

                print(traceback.format_exc())

        else:
            print('err')


def add_url(url):
    new_url = Url(url=url, is_parsed=False)

    try:

        db_session.add(new_url)
        db_session.commit()

    except sqlalchemy.exc.IntegrityError:
        # print(f' Key (url)={url}) already exists.')
        pass


    except Exception as exc:
        try:
            db_session.rollback()
            db_session.add(new_url)
            db_session.commit()
        except Exception as exc:
            db_session.rollback()
            print(traceback.format_exc())



    finally:
        db_session.close()


def get_all_website_links(url):
    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol

    print('get url')
    request_data = get_request_data(url)

    soup = BeautifulSoup(request_data.content, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue

        urls.add(href)

    return urls


# THREAD_WORKERS_AMOUNT = 5


# START_URL = 'https://github.com/semnik'
POSTGRES_ROWS_BATCH_SIZE = 100
CRAWLER_VERSION = "0.7"

print(f'crawler version - {CRAWLER_VERSION}')
print(f'THREAD_WORKERS_AMOUNT - {THREAD_WORKERS_AMOUNT}')

init_db_session()

add_url(START_URL)


while True:

    ad_records_iterable = Url.yield_not_parsed_urls(session=db_session_list)
    print('start iteration')

    # for sqlalchemy_urls_list in batch_iter(
    #         int(POSTGRES_ROWS_BATCH_SIZE / THREAD_WORKERS_AMOUNT),
    #         ad_records_iterable):
    #     threads_worker(sqlalchemy_urls_list)

    with ThreadPoolExecutor(max_workers=THREAD_WORKERS_AMOUNT) as pool:

        [pool.submit(threads_worker, sqlalchemy_urls_list)
         for sqlalchemy_urls_list in batch_iter(int(POSTGRES_ROWS_BATCH_SIZE /
                                                    THREAD_WORKERS_AMOUNT),
                                                ad_records_iterable)]

        # for url_sqlalchemy_object in sqlalchemy_urls_list:
        #     try:
        #         url_1 = url_sqlalchemy_object.url
        #     except Exception as exc:
        #         raise exc
        #
        #     if is_valid(url_1):
        #
        #
        #         try:
        #             links = get_all_website_links(url_1)
        #             update_parsed_flag(sqlalchemy_object=url_sqlalchemy_object,
        #                                session=db_session)
        #
        #             print(list(links))
        #
        #             for link in links:
        #                 add_url(link)
        #         except Exception as exc:
        #             raise exc
        #
        #             print(traceback.format_exc())
        #
        #     else:
        #         print('err')

        # ad_records_iterable = Url.yield_not_parsed_urls()
        # for urls_list in batch_iter(
        #         int(postres_rows_batch_size / thread_workers_amount),
        #         ad_records_iterable):
        #     with ThreadPoolExecutor(max_workers=thread_workers_amount) as pool:
        #         [pool.submit(get_urls, url, db_session)
        #          for url in urls_list]

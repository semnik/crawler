import itertools
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Set
from urllib.parse import urlparse, urljoin

import requests
import sqlalchemy
from bs4 import BeautifulSoup

from model import Url, db_session, init_db_session, db_session_list, \
    update_parsed_flag
from model.url import add_url
from settings import START_URL, THREAD_WORKERS_AMOUNT
from settings import print, log


def get_request_data(url: str) -> requests.Response:
    '''

    :param url:
    :return: request_data
    '''

    with requests.Session() as session:
        request_data = session.get(url, timeout=50,
                                   allow_redirects=True)

    return request_data


def is_valid(url: str):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


def get_all_website_links(url: str) -> Set[str]:
    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol

    log.debug('get url')
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

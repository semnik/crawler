import itertools
import traceback
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from settings import CONFIG_JSON
from .url import Base, Url
from settings import print

db_user = CONFIG_JSON['postgres']['user']
db_password = CONFIG_JSON['postgres']['password']
db_host = CONFIG_JSON['postgres']['host']
db_database = CONFIG_JSON['postgres']['database']
DB_CONFIG = (db_user, db_password, db_host, db_database)
DATABASE_URL = "postgresql+psycopg2://%s:%s@%s/%s" % DB_CONFIG
print(f"DATABASE_URL - {DATABASE_URL}")

db_session = scoped_session(sessionmaker())
engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)

db_session_list = Session()


def init_db_session():
    Base.metadata.create_all(engine)
    db_session.configure(bind=engine)


def update_parsed_flag(sqlalchemy_object, session: scoped_session):
    """
    updates url flag

    :param sqlalchemy_object: запись в БД в виде sqlalchemy object
    :return:
    """

    try:
        sqlalchemy_object = session.merge(sqlalchemy_object, load=False)

        sqlalchemy_object.is_parsed = True

        try:

            session.commit()

        except Exception as exc:

            session.rollback()
            print(traceback.format_exc())

    except Exception as exc:
        print(f" error - {exc}")


def batch_iter(size: int, iterable: List[str]) -> List[str]:
    """
    Creates slice of uls list of current size
    :param size: slice size
    :param iterable: list to slice
    :return: single slice per iteration
    """
    iterable = iter(iterable)
    while True:
        batch = list(itertools.islice(iterable, size))

        if not batch:
            return

        yield batch

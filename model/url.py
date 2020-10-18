import traceback

import sqlalchemy
from sqlalchemy import Column, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session

from settings import log

Base = declarative_base()


class Url(Base):
    __tablename__ = 'url'

    url = Column(Text, primary_key=True, index=True)

    is_parsed = Column(Boolean)

    @classmethod
    def get_is_parsed(cls):
        return Url.is_parsed

    @classmethod
    def find_url(cls, session, input_url):
        return session.query(Url).filter(Url.url == input_url).first()

    @classmethod
    def yield_not_parsed_urls(cls, session: scoped_session,
                              postres_rows_batch_size: int
                              = 10):
        records_list = session.query(cls).filter(
            cls.get_is_parsed() == False).yield_per(postres_rows_batch_size)

        return records_list


def add_url(url: str, db_session: scoped_session):
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
            log.error(traceback.format_exc())



    finally:
        db_session.close()

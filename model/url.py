from sqlalchemy import Column, Text, Boolean
from sqlalchemy import Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session
# from settings import log, print, db_tablename

Base = declarative_base()
ID_SEQ = Sequence('id_seq')  # define sequence explicitly

class Url(Base):
    __tablename__ = 'url'



    # id = Column(BigInteger, Sequence('id_seq'), primary_key=True, index=True)
    # id = Column(Integer, ID_SEQ, primary_key=True,
    #             server_default=ID_SEQ.next_value())



    url = Column(Text,primary_key=True, index=True)

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




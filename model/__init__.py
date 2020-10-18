from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

# from settings import CONFIG_JSON
from settings import CONFIG_JSON
from .url import Base, Url
from settings import print
db_user = CONFIG_JSON['postgres']['user']
db_password = CONFIG_JSON['postgres']['password']
db_host = CONFIG_JSON['postgres']['host']
db_database = CONFIG_JSON['postgres']['database']
DB_CONFIG = (db_user, db_password, db_host, db_database)
DATABASE_URL = "postgresql+psycopg2://%s:%s@%s/%s" % DB_CONFIG

# URI = 'postgresql+psycopg2://postgres:postgres@localhost:5434/postgres'
URI = DATABASE_URL
print(f"DATABASE_URL - {DATABASE_URL}")
# print(URI)
db_session = scoped_session(sessionmaker())
engine = create_engine(URI)

# TODO : refactor
Session = sessionmaker(bind=engine)
# Session is a class

# now session is a instance of the class Session


db_session_list = Session()


def init_db_session():
    Base.metadata.create_all(engine)
    db_session.configure(bind=engine)

from model import init_db_session, batch_iter, Url, db_session_list
from settings import THREAD_WORKERS_AMOUNT,POSTGRES_ROWS_BATCH_SIZE

if __name__ == "__main__":
    parsed_urls = set()
    init_db_session()

    urls_iterable = Url.yield_parsed_urls(
            session=db_session_list)

    for sqlalchemy_urls_list in batch_iter(int(POSTGRES_ROWS_BATCH_SIZE /
                       THREAD_WORKERS_AMOUNT),
                   urls_iterable):

        with open(f"parsed_urls.txt", "a+") as f:
            for sqlalchemy_url in sqlalchemy_urls_list:
                print(sqlalchemy_url.url.strip(), file=f)
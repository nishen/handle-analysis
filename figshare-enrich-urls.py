import multiprocessing as MP
import requests
import logging as log
from sqlalchemy import create_engine, MetaData, select, update

log.basicConfig(level=log.DEBUG, format="[%(asctime)s][%(levelname)s]: %(message)s")

s = requests.Session()

engine = create_engine("sqlite:///C:\\Data\\sqlite\\handles.db")
meta_data = MetaData(bind=engine)
MetaData.reflect(meta_data)
figshare_table = meta_data.tables['figshare']
conn = engine.connect()


def get_url(doi):
    r = s.get(f"https://doi.org/{doi}", allow_redirects=False)
    url = r.headers.get("location")
    return url

def update_url(doi):
    url = get_url(doi)
    conn.execute(update(figshare_table).where(figshare_table.c.doi == doi).values(url=url))
    log.info("updated: %s", doi)


if __name__ == "__main__":
    log.info("starting enrichment...")

    records = conn.execute(select(figshare_table.c.handle, figshare_table.c.doi))
    dois = [row[1] for row in records]
    pool = MP.Pool(processes=10)
    pool.map(update_url, dois)

    conn.close()

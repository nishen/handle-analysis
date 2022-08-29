import requests
import csv
import logging as log
from sqlalchemy import create_engine, MetaData, insert

log.basicConfig(level=log.DEBUG, format="[%(asctime)s][%(levelname)s]: %(message)s")

s = requests.Session()


def get_url(doi):
    r = s.get(f"https://doi.org/{doi}", allow_redirects=False)
    url = r.headers.get("location")
    return url


if __name__ == "__main__":
    log.info("starting extraction...")

    engine = create_engine("sqlite:///C:\\Data\\sqlite\\handles.db")
    meta_data = MetaData(bind=engine)
    MetaData.reflect(meta_data)
    figshare_table = meta_data.tables['figshare']
    conn = engine.connect()

    with open("figshare-data.csv", encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, dialect='excel')
        for row in reader:
            data = {}
            if '\n' in row[0]:
                data["pid"] = row[0].split("\n")[0]
                data["handle"] = row[0].split("\n")[1].replace("http://hdl.handle.net/", "")
                data["doi"] = row[1]
                data["article_id"] = int(row[2])
                data["url"] = get_url(row[1])
                result = conn.execute(insert(figshare_table), data)
                log.info("processed: %s", data["pid"])

    conn.close()

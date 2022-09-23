import re
import json
import logging as log
from sqlalchemy import create_engine, MetaData, insert

log.basicConfig(level=log.DEBUG, format="[%(asctime)s][%(levelname)s]: %(message)s")

if __name__ == "__main__":
    log.info("starting...")
    matcher_pid_url = re.compile('(\w+)://(.*?):?(\d{0,4})/.*/(mq:\d+)/?(.*)')
    matcher_url = re.compile("(\w+)://([a-zA-Z0-9\-\.]+):?(\d{0,5})/?.*")
    file = open("D:/Temp/handle-dump.txt", "r")
    #file = open("D:/Temp/sample-handle.txt", "r")

    # engine = create_engine("mariadb+pymysql://analytics:analytics@hermes/analytics?charset=utf8mb4")
    engine = create_engine("sqlite:///C:\\Data\\sqlite\\handles.db")
    meta_data = MetaData(bind=engine)
    MetaData.reflect(meta_data)
    handles_table = meta_data.tables['handles']
    conn = engine.connect()

    count = 0
    records = []
    data = {}
    packet = ""
    for line in file:
        line = line.rstrip()

        if line.startswith("1959.14"):
            data["handle"] = line
            continue

        packet += line

        if line == "]":
            record = json.loads(packet)
            packet = ""

            for entry in record:
                if entry["type"] == "URL":
                    data["url"] = entry["data"]["value"]
                    data["url_index"] = entry["index"]
                    if "/mq:" in data["url"]:
                        groups_url = matcher_pid_url.match(data["url"])
                        data["scheme"] = groups_url.group(1)
                        data["host"] = groups_url.group(2)
                        data["port"] = int(groups_url.group(3)) if groups_url.group(3) != '' else 80
                        data["pid"] = groups_url.group(4)
                        data["datastream"] = groups_url.group(5) if len(groups_url.group(5)) > 0 else None
                    else:
                        groups_url = matcher_url.match(data["url"])
                        data["scheme"] = groups_url.group(1)
                        data["host"] = groups_url.group(2)
                        data["port"] = int(groups_url.group(3)) if groups_url.group(3) != '' else 80
                        data["pid"] = None
                        data["datastream"] = None
                elif entry["type"] == "HS_ALIAS":
                    data["alias"] = entry["data"]["value"]
                    data["alias_index"] = entry["index"]

            if "alias" not in data:
                data["alias"] = None
                data["alias_index"] = None

            if "url" not in data:
                data["url"] = None
                data["url_index"] = None
                data["scheme"] = None
                data["host"] = None
                data["port"] = None
                data["pid"] = None
                data["datastream"] = None
            records.append(data)
            data = {}

        if len(records) >= 20000:
            count += len(records)
            log.info("adding records: %s", count)
            result = conn.execute(insert(handles_table), records)
            records.clear()

    if len(records) > 0:
        count += len(records)
        log.info("adding records: %s", count)
        result = conn.execute(insert(handles_table), records)
        count += len(records)
        records.clear()

    file.close()
    conn.close()

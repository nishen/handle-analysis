import logging as log
import json
import re

log.basicConfig(level=log.DEBUG, format="[%(asctime)s][%(levelname)s]: %(message)s")

if __name__ == "__main__":
    matcher_url = re.compile('(\\w+)://(.*?):?(\\d{0,4})/.*/(mq:\\d+)/?(.*)')

    file = open("D:/Temp/sample-handle.txt", "r")

    records = []
    data = {}
    packet = ""
    for line in file:
        line = line.rstrip()

        if line.startswith("1959.14"):
            data["handle"] = line.rstrip()
            continue

        packet += line

        if line == "]":
            record = json.loads(packet)
            for entry in record:
                if entry["type"] == "URL":
                    data["url"] = entry["data"]["value"]
                    data["url_index"] = entry["index"]
                    if "/mq:" in data["url"]:
                        groups_url = matcher_url.match(data["url"])
                        data["scheme"] = groups_url.group(1)
                        data["host"] = groups_url.group(2)
                        data["port"] = int(groups_url.group(3))
                        data["pid"] = groups_url.group(4)
                        data["datastream"] = groups_url.group(5) if len(groups_url.group(5)) > 0 else None
                    else:
                        data["scheme"] = None
                        data["host"] = None
                        data["port"] = None
                        data["pid"] = None
                        data["datastream"] = None
                    records.append(data)
                    data = {}
                    packet = ""
        
    log.info(records)
